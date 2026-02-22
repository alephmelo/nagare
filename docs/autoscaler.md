# Autoscaler — How to Use

Nagare's autoscaler watches the queue depth of each worker pool and automatically spins up cloud workers when work is piling up, then tears them down when they go idle. From a user's perspective it is invisible: DAGs and tasks work exactly the same whether a task runs on a local worker or a cloud-provisioned one.

---

## How it works

Every 30 seconds the autoscaler evaluates each pool:

1. **Scale up** — if `queued_tasks > scale_up_threshold` and the global cloud worker cap has not been reached and the per-pool cooldown has expired, one new worker is started.
2. **Scale down** — if a running cloud worker's pools have had zero queued tasks for `scale_down_idle_mins` minutes, the worker is terminated.

Workers are ordinary `nagare --worker` processes. The autoscaler handles the entire lifecycle (start, register correlation, idle detection, teardown) and persists instance state in the `cloud_instances` SQLite table so it survives master restarts.

---

## Quick start — Docker provider (local dev)

### 1. Build the worker image

The Docker provider starts workers by running `nagare --worker …` inside a container. The image must have the `nagare` binary on `PATH`.

Create a `Dockerfile.worker` at the project root:

```dockerfile
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY nagare /usr/local/bin/nagare
RUN chmod +x /usr/local/bin/nagare
ENTRYPOINT []
CMD []
```

Build it (run `make build` first to produce the `nagare` binary):

```bash
make build
docker build -t nagare:latest -f Dockerfile.worker .
```

### 2. Enable the autoscaler in `nagare.yaml`

Uncomment the `autoscaler` block (it is pre-populated with sensible local defaults):

```yaml
autoscaler:
  enabled: true
  provider: "docker"
  scale_up_threshold: 3      # spin up when > 3 tasks are queued in a pool
  max_cloud_workers: 5       # never run more than 5 cloud workers at once
  scale_down_idle_mins: 2    # tear down after 2 minutes of idle
  cooldown_secs: 30          # wait 30 s before spinning up another worker for the same pool

  docker:
    image: "nagare:latest"
    network: "host"          # lets the worker container reach master at localhost:8080
```

> **Network note:** `network: "host"` is the simplest setup — the worker container shares the host network stack and can reach the master at `localhost:8080`.  If you run the master inside Docker too, create a shared bridge network and set `network: "nagare-net"` instead.

### 3. Start the master

```bash
./nagare
# or during development:
make dev
```

You should see:

```
Autoscaler: initialized (provider=docker, max=5)
Autoscaler: started (provider=docker, max=5, threshold=3, cooldown=30s)
```

### 4. Trigger the stress-test DAG

```bash
curl -X POST http://localhost:8080/api/dags/autoscale_stress_test/trigger
```

Or click **Trigger** next to `autoscale_stress_test` in the dashboard.

This submits 8 slow tasks (45 s each) to the `default` pool. Within the next autoscaler tick (~30 s) you should see:

```
Autoscaler: scaling up pool default (queued=8, threshold=3) → instance docker-a1b2c3
Cluster: cloud worker worker-hostname-12345 registered (instance=docker-a1b2c3, pools=[default])
```

And in `docker ps`:

```
CONTAINER ID   IMAGE           COMMAND                  ...
a1b2c3d4e5f6   nagare:latest   "nagare --worker --j…"   ...
```

### 5. Watch the autoscaler status endpoint

```bash
curl http://localhost:8080/api/autoscaler/status | jq
```

```json
{
  "enabled": true,
  "provider": "docker",
  "cloud_workers": 2,
  "max_cloud_workers": 5,
  "pools": {
    "default": {
      "pool": "default",
      "queued_tasks": 5,
      "active_workers": 6,
      "cloud_workers": 2
    }
  },
  "instances": [
    {
      "id": "docker-a1b2c3",
      "provider_id": "a1b2c3d4e5f6...",
      "worker_id": "worker-hostname-12345",
      "pools": ["default"],
      "status": "running",
      "cost_per_hour": 0,
      "created_at": "2026-02-22T10:00:00Z"
    }
  ]
}
```

### 6. Watch scale-down

After all tasks finish and the pool goes idle for `scale_down_idle_mins` (2 minutes in the example config), you will see:

```
Autoscaler: scaling down idle instance docker-a1b2c3 (provider=container-..., idle=2m05s)
```

The container is removed and the instance disappears from `docker ps`.

---

## Configuration reference

All fields live under the `autoscaler:` key in `nagare.yaml`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Master switch. Set to `true` to activate. |
| `provider` | string | — | `"docker"` or `"aws"`. Required when enabled. |
| `scale_up_threshold` | int | `3` | Queued-task count that triggers a scale-up for the pool. |
| `max_cloud_workers` | int | `5` | Global ceiling on simultaneously running cloud workers. |
| `scale_down_idle_mins` | int | `10` | Minutes a worker must be idle before it is terminated. |
| `cooldown_secs` | int | `60` | Minimum seconds between consecutive scale-ups for the same pool. Prevents burst thrashing. |
| `docker.image` | string | `"nagare:latest"` | Docker image containing the `nagare` binary. |
| `docker.network` | string | `"host"` | Docker network mode for worker containers. |
| `aws.region` | string | — | AWS region (e.g. `"us-east-1"`). |
| `aws.instance_type` | string | — | EC2 instance type for CPU workers (e.g. `"t3.medium"`). |
| `aws.gpu_instance_type` | string | — | EC2 instance type when a task sets `resources.gpus` (e.g. `"g4dn.xlarge"`). |
| `aws.ami_id` | string | — | AMI ID with `nagare` binary pre-installed. |
| `aws.key_name` | string | — | EC2 key pair name for SSH access (optional). |
| `aws.security_group` | string | — | Security group ID. Must allow outbound to master:8080. |
| `aws.subnet_id` | string | — | Subnet to launch instances in. |
| `aws.iam_instance_profile` | string | — | IAM instance profile ARN or name for the worker instances. |
| `aws.nagare_download_url` | string | — | URL the EC2 user-data script downloads the `nagare` binary from. |

---

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/autoscaler/status` | Current snapshot: enabled flag, provider, per-pool stats, instance list. |
| `GET` | `/api/autoscaler/costs` | Estimated cloud spend summary (total hours, estimated USD). |
| `POST` | `/api/autoscaler/enable` | Toggle autoscaler on/off at runtime without restarting. Body: `{"enabled": true}`. |

---

## Assigning tasks to specific pools

Cloud workers only pick up tasks whose `pool` matches the pools the worker was started with. The autoscaler always starts a worker for the pool that triggered the scale-up.

```yaml
# dags/my_dag.yaml
tasks:
  - id: cpu_task
    type: command
    pool: default         # handled by default-pool workers (local or cloud)
    command: "python train.py"

  - id: gpu_task
    type: command
    pool: gpu_workers     # handled only by gpu_workers-pool workers
    command: "python infer.py"
    depends_on: [cpu_task]
```

To have the autoscaler also cover `gpu_workers`, make sure that pool is listed in `worker_pools` in `nagare.yaml` and that there are enough queued GPU tasks to cross the threshold:

```yaml
worker_pools:
  default: 2
  gpu_workers: 0          # 0 local workers — rely entirely on cloud workers for this pool
```

---

## Troubleshooting

**Workers spin up but never register**

- Check that the container can reach the master. With `network: "host"` the master must listen on `0.0.0.0` (the default `:8080` bind does this).
- Run `docker logs <container-id>` to see the worker's startup output.
- If using a token (`--token`), verify the same value is in `nagare.yaml` under the `--token` flag or passed to the master process.

**Scale-up never triggers**

- Confirm `enabled: true` is not commented out.
- Check the 30-second autoscaler tick cadence — trigger a run and wait up to 30 s.
- Verify `scale_up_threshold` is lower than the number of queued tasks. The threshold is *strictly greater than* (`>`), so with `scale_up_threshold: 3` you need at least 4 queued tasks.

**Workers are terminated mid-task**

- Increase `scale_down_idle_mins`. The idle timer resets whenever a task is queued in the pool the worker serves, but if all tasks finish before the next tick the timer may start prematurely.
- The autoscaler only terminates `InstanceRunning` workers that it provisioned itself. Manually started workers are never touched.

**`docker: create client` error on startup**

- The Docker daemon must be running. On macOS: open Docker Desktop.
- Verify with `docker info`.

---

## Production — AWS EC2 provider

> The AWS SDK dependency (`github.com/aws/aws-sdk-go-v2/...`) is not yet bundled. `NewAWSProvider` returns a stub error until the dependency is added. The config fields and interface are complete — wiring up the real EC2 client is the remaining TODO.

When the AWS provider is wired in:

1. Build an AMI that has the `nagare` binary at `/usr/local/bin/nagare` and `cloud-init` installed.
2. Set `aws.nagare_download_url` if you prefer downloading at boot time instead of baking into the AMI.
3. The EC2 user-data script (generated by the AWS provider) runs:
   ```bash
   nagare --worker --join http://<master-public-ip>:8080 --pools <pool> --token <token>
   ```
4. Ensure the worker instances' security group allows outbound TCP to the master on port 8080.
5. The IAM instance profile must allow `ec2:DescribeInstances` (for reconciliation) — no other AWS permissions are needed for the workers themselves.
