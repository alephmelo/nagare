"use client";

import { useEffect, useState } from "react";
import {
  Title,
  Card,
  Table,
  Badge,
  Text,
  Group,
  Button,
  Skeleton,
  Stack,
  ThemeIcon,
  SimpleGrid,
  Alert,
  Progress,
  Tooltip,
} from "@mantine/core";
import {
  IconRefresh,
  IconCloudComputing,
  IconCircleCheck,
  IconCircleDashed,
  IconCircleX,
  IconInfoCircle,
  IconCurrencyDollar,
  IconServer,
  IconClockHour4,
} from "@tabler/icons-react";
import { apiFetch } from "../../lib/apiFetch";

// ── Types ─────────────────────────────────────────────────────────────────────

interface PoolStats {
  pool: string;
  queued_tasks: number;
  active_workers: number;
  cloud_workers: number;
  needs_gpu: boolean;
}

interface WorkerInstance {
  id: string;
  provider_id: string;
  status: string; // "provisioning" | "running" | "terminated"
  pools: string[];
  worker_id: string;
  needs_gpu: boolean;
  cost_per_hour: number;
  created_at: string;
  terminated_at: string | null;
}

interface StatusSnapshot {
  enabled: boolean;
  provider: string;
  cloud_workers: number;
  max_cloud_workers: number;
  pools: Record<string, PoolStats>;
  instances: WorkerInstance[];
}

interface CostSummary {
  total_instances: number;
  active_instances: number;
  estimated_cost_usd: number;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function uptime(createdAt: string): string {
  const secs = Math.floor((Date.now() - new Date(createdAt).getTime()) / 1000);
  if (secs < 60) return `${secs}s`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m`;
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  return m > 0 ? `${h}h ${m}m` : `${h}h`;
}

function instanceStatusBadge(status: string) {
  switch (status) {
    case "running":
      return (
        <Badge
          color="green"
          variant="light"
          size="sm"
          radius="sm"
          leftSection={
            <IconCircleCheck
              size={12}
              style={{ display: "flex", alignItems: "center", marginTop: "2px" }}
            />
          }
        >
          Running
        </Badge>
      );
    case "provisioning":
      return (
        <Badge
          color="yellow"
          variant="light"
          size="sm"
          radius="sm"
          leftSection={
            <IconCircleDashed
              size={12}
              style={{ display: "flex", alignItems: "center", marginTop: "2px" }}
            />
          }
        >
          Provisioning
        </Badge>
      );
    default:
      return (
        <Badge
          color="gray"
          variant="light"
          size="sm"
          radius="sm"
          leftSection={
            <IconCircleX
              size={12}
              style={{ display: "flex", alignItems: "center", marginTop: "2px" }}
            />
          }
        >
          {status.charAt(0).toUpperCase() + status.slice(1)}
        </Badge>
      );
  }
}

// ── Table row sub-components ──────────────────────────────────────────────────
// Extracted so that pools.map / sortedInstances.map returns a simple array of
// <PoolRow key=…> / <InstanceRow key=…> elements. React 19 warns when an
// inline JSX block inside a Mantine Box/Tr receives a raw array as children;
// named components avoid that by deferring child rendering one level down.

function PoolRow({ p }: { p: PoolStats }) {
  const barValue = Math.min((p.queued_tasks / Math.max(p.queued_tasks, 5)) * 100, 100);
  const pressure = p.queued_tasks === 0 ? "green" : p.queued_tasks < 3 ? "yellow" : "red";
  return (
    <Table.Tr>
      <Table.Td>
        <Group gap={6}>
          <Badge variant="outline" color="teal" size="sm" radius="sm">
            {p.pool}
          </Badge>
          {p.needs_gpu && (
            <Badge variant="outline" color="violet" size="sm" radius="sm">
              GPU
            </Badge>
          )}
        </Group>
      </Table.Td>
      <Table.Td>
        <Text size="sm" fw={p.queued_tasks > 0 ? 600 : 400}>
          {p.queued_tasks}
        </Text>
      </Table.Td>
      <Table.Td>
        <Text size="sm">{p.active_workers}</Text>
      </Table.Td>
      <Table.Td>
        <Text size="sm" c={p.cloud_workers > 0 ? "cyan" : "dimmed"}>
          {p.cloud_workers}
        </Text>
      </Table.Td>
      <Table.Td style={{ minWidth: 120 }}>
        <Tooltip
          label={`${p.queued_tasks} task${p.queued_tasks !== 1 ? "s" : ""} queued`}
          position="right"
        >
          <Progress value={barValue} color={pressure} size="sm" radius="xl" />
        </Tooltip>
      </Table.Td>
    </Table.Tr>
  );
}

function InstanceRow({ inst }: { inst: WorkerInstance }) {
  return (
    <Table.Tr>
      <Table.Td>
        <Text size="sm" ff="monospace">
          {inst.id}
        </Text>
      </Table.Td>
      <Table.Td>
        <Text size="sm" ff="monospace" c="dimmed">
          {inst.provider_id || "—"}
        </Text>
      </Table.Td>
      <Table.Td>{instanceStatusBadge(inst.status)}</Table.Td>
      <Table.Td>
        <Group gap={4}>
          {(inst.pools ?? []).map((pool) => (
            <Badge key={pool} variant="outline" color="teal" size="sm" radius="sm">
              {pool}
            </Badge>
          ))}
        </Group>
      </Table.Td>
      <Table.Td>
        <Text size="sm" ff="monospace" c={inst.worker_id ? undefined : "dimmed"}>
          {inst.worker_id || "not yet registered"}
        </Text>
      </Table.Td>
      <Table.Td>
        <Group gap={4}>
          <IconClockHour4 size={14} color="var(--mantine-color-dimmed)" />
          <Text size="sm" c="dimmed">
            {inst.terminated_at
              ? uptime(inst.created_at) + " (terminated)"
              : uptime(inst.created_at)}
          </Text>
        </Group>
      </Table.Td>
      <Table.Td>
        <Text size="sm" c={inst.cost_per_hour > 0 ? undefined : "dimmed"}>
          {inst.cost_per_hour > 0 ? `$${inst.cost_per_hour.toFixed(3)}` : "—"}
        </Text>
      </Table.Td>
    </Table.Tr>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────

export default function AutoscalerPage() {
  const [status, setStatus] = useState<StatusSnapshot | null>(null);
  const [costs, setCosts] = useState<CostSummary | null>(null);
  const [loading, setLoading] = useState(true);
  const [notConfigured, setNotConfigured] = useState(false);

  const fetchData = async () => {
    try {
      const [statusRes, costsRes] = await Promise.all([
        apiFetch("/api/autoscaler/status"),
        apiFetch("/api/autoscaler/costs"),
      ]);

      if (statusRes.status === 503) {
        setNotConfigured(true);
        return;
      }

      if (statusRes.ok) {
        const s: StatusSnapshot = await statusRes.json();
        setStatus(s);
        setNotConfigured(false);
      }

      if (costsRes.ok) {
        const c: CostSummary = await costsRes.json();
        setCosts(c);
      }
    } catch (err) {
      console.error("Failed to fetch autoscaler data", err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    // If the autoscaler is not configured the first fetch will set notConfigured=true.
    // We use a ref so the interval callback can see the latest value without
    // being re-created (which would restart the timer on every render).
    const interval = setInterval(() => {
      setNotConfigured((nc) => {
        if (!nc) fetchData();
        return nc;
      });
    }, 5000);
    return () => clearInterval(interval);
  }, []);

  const pools = status ? Object.values(status.pools) : [];
  const instances = status?.instances ?? [];
  // Show active (non-terminated) instances first, then sort by created_at desc.
  const sortedInstances = [...instances].sort((a, b) => {
    if (a.status === "terminated" && b.status !== "terminated") return 1;
    if (a.status !== "terminated" && b.status === "terminated") return -1;
    return new Date(b.created_at).getTime() - new Date(a.created_at).getTime();
  });

  return (
    <>
      <Group justify="space-between" mb="xl">
        <Title order={2}>Autoscaler</Title>
        <Button leftSection={<IconRefresh size={16} />} variant="light" onClick={fetchData}>
          Refresh
        </Button>
      </Group>

      {notConfigured ? (
        <Alert
          variant="light"
          color="blue"
          icon={<IconInfoCircle />}
          title="Autoscaler not enabled"
        >
          The autoscaler is disabled. To enable it, add an{" "}
          <Text span ff="monospace" size="sm">
            autoscaler:
          </Text>{" "}
          block to your{" "}
          <Text span ff="monospace" size="sm">
            nagare.yaml
          </Text>{" "}
          and set{" "}
          <Text span ff="monospace" size="sm">
            enabled: true
          </Text>
          . See{" "}
          <Text span ff="monospace" size="sm">
            docs/autoscaler.md
          </Text>{" "}
          for full configuration reference.
        </Alert>
      ) : loading && !status ? (
        <Stack gap="md">
          <Skeleton height={80} radius="md" />
          <Skeleton height={200} radius="md" />
          <Skeleton height={200} radius="md" />
        </Stack>
      ) : (
        <>
          {/* ── Stat cards ─────────────────────────────────────────────────── */}
          <SimpleGrid cols={{ base: 2, sm: 4 }} mb="xl">
            <Card padding="md">
              <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                Status
              </Text>
              <Group gap="xs" mt={4}>
                <ThemeIcon variant="light" color={status?.enabled ? "green" : "gray"} size="sm">
                  {status?.enabled ? <IconCircleCheck size={14} /> : <IconCircleX size={14} />}
                </ThemeIcon>
                <Text fw={700} size="xl" c={status?.enabled ? "green" : "dimmed"}>
                  {status?.enabled ? "Enabled" : "Disabled"}
                </Text>
              </Group>
            </Card>

            <Card padding="md">
              <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                Provider
              </Text>
              <Group gap="xs" mt={4}>
                <ThemeIcon variant="light" color="cyan" size="sm">
                  <IconCloudComputing size={14} />
                </ThemeIcon>
                <Text fw={700} size="xl" tt="capitalize">
                  {status?.provider || "—"}
                </Text>
              </Group>
            </Card>

            <Card padding="md">
              <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                Cloud Workers
              </Text>
              <Group gap="xs" mt={4}>
                <ThemeIcon variant="light" color="blue" size="sm">
                  <IconServer size={14} />
                </ThemeIcon>
                <Text fw={700} size="xl">
                  {status?.cloud_workers ?? 0}
                  <Text span size="sm" c="dimmed" fw={400}>
                    {" "}
                    / {status?.max_cloud_workers ?? 0}
                  </Text>
                </Text>
              </Group>
            </Card>

            <Card padding="md">
              <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                Est. Cost (USD)
              </Text>
              <Group gap="xs" mt={4}>
                <ThemeIcon variant="light" color="teal" size="sm">
                  <IconCurrencyDollar size={14} />
                </ThemeIcon>
                <Text fw={700} size="xl">
                  ${costs?.estimated_cost_usd?.toFixed(4) ?? "0.0000"}
                </Text>
              </Group>
            </Card>
          </SimpleGrid>

          {/* ── Pool pressure ───────────────────────────────────────────────── */}
          <Title order={4} mb="md" c="dimmed">
            Pool Pressure
          </Title>
          <Card padding="0" mb="xl">
            <Table.ScrollContainer minWidth={500}>
              <Table verticalSpacing="sm" horizontalSpacing="md" striped highlightOnHover>
                <Table.Thead>
                  <Table.Tr>
                    {["Pool", "Queued Tasks", "Active Workers", "Cloud Workers", "Queue Depth"].map(
                      (h) => (
                        <Table.Th key={h} style={{ borderBottom: "2px solid var(--border-color)" }}>
                          <Text size="sm" fw={700}>
                            {h}
                          </Text>
                        </Table.Th>
                      )
                    )}
                  </Table.Tr>
                </Table.Thead>
                <Table.Tbody>
                  {pools.length === 0 ? (
                    <Table.Tr>
                      <Table.Td colSpan={5}>
                        <Text c="dimmed" ta="center" py="md">
                          No pool data available.
                        </Text>
                      </Table.Td>
                    </Table.Tr>
                  ) : (
                    pools.map((p) => <PoolRow key={p.pool} p={p} />)
                  )}
                </Table.Tbody>
              </Table>
            </Table.ScrollContainer>
          </Card>

          {/* ── Cloud instances ─────────────────────────────────────────────── */}
          <Title order={4} mb="md" c="dimmed">
            Cloud Instances
          </Title>
          <Card padding="0">
            <Table.ScrollContainer minWidth={700}>
              <Table verticalSpacing="sm" horizontalSpacing="md" striped highlightOnHover>
                <Table.Thead>
                  <Table.Tr>
                    {[
                      "Instance ID",
                      "Provider ID",
                      "Status",
                      "Pools",
                      "Worker ID",
                      "Uptime",
                      "Cost / hr",
                    ].map((h) => (
                      <Table.Th key={h} style={{ borderBottom: "2px solid var(--border-color)" }}>
                        <Text size="sm" fw={700}>
                          {h}
                        </Text>
                      </Table.Th>
                    ))}
                  </Table.Tr>
                </Table.Thead>
                <Table.Tbody>
                  {sortedInstances.length === 0 ? (
                    <Table.Tr>
                      <Table.Td colSpan={7}>
                        <Text c="dimmed" ta="center" py="md">
                          No cloud instances provisioned yet.
                        </Text>
                      </Table.Td>
                    </Table.Tr>
                  ) : (
                    sortedInstances.map((inst) => <InstanceRow key={inst.id} inst={inst} />)
                  )}
                </Table.Tbody>
              </Table>
            </Table.ScrollContainer>
          </Card>
        </>
      )}
    </>
  );
}
