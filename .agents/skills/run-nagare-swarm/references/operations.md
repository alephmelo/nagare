# Nagare swarm operations

## Safety model

Nagare runs from a versioned release under `SWARM_HOME` (falling back to `$XDG_DATA_HOME/nagare-swarm`, then `~/.local/share/nagare-swarm`). The release contains an exact allowlist of the binary, DAG, backlog, runner, schemas, config, and manifest. The launcher verifies every checksum before executing it. Development builds and later checkout edits cannot change a running release.

Codex sessions use saved CLI authentication but ignore user configuration and disable apps, plugins, remote plugins, and the in-app browser. Child environments use an explicit allowlist. Model-generated commands run with `--ask-for-approval never` and either `read-only` or `workspace-write`; failed permissions are returned to the model and cannot pause an unattended run. The launcher removes the local Nagare API-key file immediately after a successful trigger, so workers cannot recover the control-plane credential from disk. The running daemon and authenticated browser retain it; restarting Nagare rotates it.

Agent-authored checks run in a second OS sandbox with no external network, an isolated home/cache, hard denials for credentials and swarm state, confined writes, no process inspection or clipboard access, and loopback only for test servers. Every Codex session and check gets its own process group; the runner terminates the complete group before validating mutations or advancing state.

The final output is a local `codex/architecture-swarm-<run-id>` branch. Nothing publishes automatically.

## Install and validate

Build from a normal clean clone of the intended commit into a temporary path, not `./nagare`. VCS stamping is mandatory:

```bash
tmp_dir="$(mktemp -d)"
cd web && npm run build
cd ..
go build -buildvcs=true -o "$tmp_dir/nagare" .
scripts/nagare-swarm install --binary "$tmp_dir/nagare"
scripts/nagare-swarm validate
```

The installer rejects a binary unless its module path, source revision, and `vcs.modified=false` stamp match the checkout. `validate --ready` additionally requires the swarm control files to be tracked and the primary checkout to have no modified tracked files. Untracked files do not enter agent worktrees.

## Run and observe

```bash
scripts/nagare-swarm start
scripts/nagare-swarm copy-api-key
scripts/nagare-swarm trigger
```

Nagare allows up to two path-disjoint tasks in the same wave to run concurrently. Integration is serialized, and later tasks start only after every preceding wave passes the full checks.

The local UI is available at `http://127.0.0.1:18080` while the control plane is running. Before triggering, run `scripts/nagare-swarm copy-api-key` and paste the copied value. Keep that authenticated browser session open to monitor workers after launch; CLI API commands are intentionally unavailable once the key is sealed.

## Recovery

- `validate --ready` reports untracked or modified control files: review and commit the swarm control plane first, rebuild the binary from that clean commit, install a new pinned release, and rerun readiness. Never bypass this preflight.
- Nagare stopped: run `start`, then `trigger`. The active state is reused.
- A Nagare task timed out or the process crashed: inspect `status` and `logs`, then retrigger. Completed runner stages are idempotent.
- An agent/check/reviewer failed: inspect the preserved worktree and `${SWARM_HOME}/runs/<run-id>/artifacts`. Fix the backlog or code only with explicit user authorization, pin a new release if control files changed, then retrigger.
- Integration conflict: inspect the preserved integration worktree. Do not resolve by dropping tests or bypassing checks.
- Wrong base commit while a Nagare run is active: finish or cancel the old run before moving the primary checkout. Once Nagare reports no active run, `trigger` archives the superseded runner pointer and preserves its state and worktrees before starting from the new committed `HEAD`.
- Stop scheduling: `scripts/nagare-swarm cancel RUN_ID`, followed by `scripts/nagare-swarm stop` if the control plane should also stop. Runner state and worktrees remain recoverable.

The runner deliberately has no push, merge-to-main, tag, release, or deploy command.
