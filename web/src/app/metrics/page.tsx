"use client";

import { useEffect, useState, useCallback } from "react";
import { apiFetch } from "../../lib/apiFetch";
import { useVisibilityPoll } from "../../lib/useVisibilityPoll";
import {
  Title,
  Card,
  Badge,
  Group,
  Text,
  Stack,
  Select,
  Table,
  SimpleGrid,
  Skeleton,
  ThemeIcon,
  Anchor,
  Box,
  SegmentedControl,
  Tooltip,
  Progress,
} from "@mantine/core";
import {
  IconClock,
  IconCpu,
  IconDatabase,
  IconCheck,
  IconChartBar,
  IconRefresh,
  IconTrendingUp,
} from "@tabler/icons-react";
import { AreaChart, BarChart } from "@mantine/charts";
import { useRouter } from "next/navigation";

// ---- Types ------------------------------------------------------------------

interface TaskMetrics {
  TaskInstanceID: string;
  RunID: string;
  DAGID: string;
  TaskID: string;
  DurationMs: number;
  CpuUserMs: number;
  CpuSystemMs: number;
  PeakMemoryBytes: number;
  ExitCode: number;
  ExecutorType: string;
  CreatedAt: string;
}

interface TimeSeriesPoint {
  timestamp: string;
  duration_ms: number;
  memory_bytes: number;
  cpu_ms: number;
  task_id: string;
  run_id: string;
  status: string;
}

interface DagStat {
  dag_id: string;
  count: number;
  avg_duration_ms: number;
  max_duration_ms: number;
  avg_memory_bytes: number;
  max_memory_bytes: number;
  success_rate: number;
}

interface SlowestTask {
  task_instance_id: string;
  dag_id: string;
  task_id: string;
  run_id: string;
  duration_ms: number;
  peak_memory_bytes: number;
  exit_code: number;
  created_at: string;
}

interface OverviewData {
  total_tasks: number;
  avg_duration_ms: number;
  max_memory_bytes: number;
  total_cpu_ms: number;
  success_rate: number;
  dag_stats: DagStat[] | null;
  slowest_tasks: SlowestTask[] | null;
}

// ---- Helpers ----------------------------------------------------------------

function fmtDuration(ms: number): string {
  if (ms <= 0) return "—";
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  const mins = Math.floor(ms / 60000);
  const secs = Math.round((ms % 60000) / 1000);
  return `${mins}m ${secs}s`;
}

function fmtMemory(bytes: number): string {
  if (bytes <= 0) return "—";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function fmtPercent(rate: number): string {
  return `${(rate * 100).toFixed(1)}%`;
}

// ---- Stat Card --------------------------------------------------------------

function StatCard({
  label,
  value,
  icon,
  color,
  sub,
}: {
  label: string;
  value: string;
  icon: React.ReactNode;
  color: string;
  sub?: string;
}) {
  return (
    <Card withBorder padding="md">
      <Group justify="space-between" wrap="nowrap">
        <div>
          <Text size="xs" c="dimmed" tt="uppercase" fw={600} mb={4}>
            {label}
          </Text>
          <Text fw={700} size="xl">
            {value}
          </Text>
          {sub && (
            <Text size="xs" c="dimmed" mt={2}>
              {sub}
            </Text>
          )}
        </div>
        <ThemeIcon color={color} variant="light" size={48} radius="md">
          {icon}
        </ThemeIcon>
      </Group>
    </Card>
  );
}

// ---- Main Page --------------------------------------------------------------

export default function MetricsPage() {
  const router = useRouter();
  const [since, setSince] = useState("24h");
  const [dagFilter, setDagFilter] = useState<string | null>(null);
  const [overview, setOverview] = useState<OverviewData | null>(null);
  const [series, setSeries] = useState<TimeSeriesPoint[]>([]);
  const [dags, setDags] = useState<{ value: string; label: string }[]>([]);
  const [loading, setLoading] = useState(true);

  const fetchData = useCallback(async () => {
    try {
      const [overviewRes, seriesRes, dagsRes] = await Promise.all([
        apiFetch(`/api/metrics/overview?since=${since}`),
        apiFetch(
          `/api/metrics/timeseries?since=${since}&limit=500${dagFilter ? `&dag_id=${dagFilter}` : ""}`
        ),
        apiFetch("/api/dags"),
      ]);

      if (overviewRes.ok) setOverview(await overviewRes.json());
      if (seriesRes.ok) setSeries((await seriesRes.json()) ?? []);
      if (dagsRes.ok) {
        const dagList: { ID: string }[] = await dagsRes.json();
        setDags(dagList.map((d) => ({ value: d.ID, label: d.ID })));
      }
    } catch {
      // noop — keep stale data
    } finally {
      setLoading(false);
    }
  }, [since, dagFilter]);

  useVisibilityPoll(fetchData, 15000, [fetchData]);

  // ---- Transform time-series for charts -----------------------------------

  // Duration area chart: each point is { time, duration_ms }
  const durationChartData = series.map((p) => ({
    time: new Date(p.timestamp).toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
    }),
    "Duration (ms)": p.duration_ms,
  }));

  // Memory bar chart
  const memoryChartData = series.map((p) => ({
    time: new Date(p.timestamp).toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
    }),
    "Memory (MB)": +(p.memory_bytes / (1024 * 1024)).toFixed(2),
  }));

  // CPU area chart
  const cpuChartData = series.map((p) => ({
    time: new Date(p.timestamp).toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
    }),
    "CPU (ms)": p.cpu_ms,
  }));

  const dagStats = overview?.dag_stats ?? [];
  const slowestTasks = overview?.slowest_tasks ?? [];

  return (
    <Stack gap="lg">
      {/* Header */}
      <Group justify="space-between">
        <Group gap="sm">
          <ThemeIcon color="violet" variant="light" size={36} radius="md">
            <IconChartBar size={20} />
          </ThemeIcon>
          <Title order={2}>Metrics</Title>
        </Group>
        <Group gap="sm">
          <Select
            size="xs"
            placeholder="All DAGs"
            clearable
            data={dags}
            value={dagFilter}
            onChange={setDagFilter}
            w={180}
          />
          <SegmentedControl
            size="xs"
            value={since}
            onChange={setSince}
            data={[
              { label: "1h", value: "1h" },
              { label: "6h", value: "6h" },
              { label: "24h", value: "24h" },
              { label: "7d", value: "7d" },
              { label: "30d", value: "30d" },
            ]}
          />
          <Tooltip label="Refresh">
            <ThemeIcon
              variant="default"
              size="sm"
              style={{ cursor: "pointer" }}
              onClick={fetchData}
            >
              <IconRefresh size={14} />
            </ThemeIcon>
          </Tooltip>
        </Group>
      </Group>

      {/* Overview Stat Cards */}
      <SimpleGrid cols={{ base: 2, sm: 2, lg: 4 }} spacing="sm">
        {loading ? (
          Array.from({ length: 4 }).map((_, i) => <Skeleton key={i} height={88} radius="md" />)
        ) : (
          <>
            <StatCard
              label="Tasks Tracked"
              value={String(overview?.total_tasks ?? 0)}
              icon={<IconTrendingUp size={22} />}
              color="blue"
              sub={`last ${since}`}
            />
            <StatCard
              label="Avg Task Duration"
              value={fmtDuration(overview?.avg_duration_ms ?? 0)}
              icon={<IconClock size={22} />}
              color="teal"
            />
            <StatCard
              label="Peak Memory Seen"
              value={fmtMemory(overview?.max_memory_bytes ?? 0)}
              icon={<IconDatabase size={22} />}
              color="grape"
            />
            <StatCard
              label="Success Rate"
              value={fmtPercent(overview?.success_rate ?? 0)}
              icon={<IconCheck size={22} />}
              color={(overview?.success_rate ?? 1) >= 0.95 ? "green" : "orange"}
              sub={`CPU total: ${fmtDuration(overview?.total_cpu_ms ?? 0)}`}
            />
          </>
        )}
      </SimpleGrid>

      {/* Charts Row */}
      <SimpleGrid cols={{ base: 1, md: 2 }} spacing="sm">
        {/* Duration over time */}
        <Card withBorder padding="md">
          <Text fw={600} size="sm" mb="md">
            Task Duration Over Time
          </Text>
          {loading || series.length === 0 ? (
            <Box h={200}>
              {loading ? (
                <Skeleton height={200} radius="sm" />
              ) : (
                <Text c="dimmed" ta="center" pt={80} size="sm">
                  No data for this time range
                </Text>
              )}
            </Box>
          ) : (
            <AreaChart
              h={200}
              data={durationChartData}
              dataKey="time"
              series={[{ name: "Duration (ms)", color: "teal.6" }]}
              curveType="monotone"
              withDots={false}
              gridAxis="xy"
              tickLine="xy"
              strokeWidth={2}
            />
          )}
        </Card>

        {/* Memory usage */}
        <Card withBorder padding="md">
          <Text fw={600} size="sm" mb="md">
            Peak Memory Usage (MB)
          </Text>
          {loading || series.length === 0 ? (
            <Box h={200}>
              {loading ? (
                <Skeleton height={200} radius="sm" />
              ) : (
                <Text c="dimmed" ta="center" pt={80} size="sm">
                  No data for this time range
                </Text>
              )}
            </Box>
          ) : (
            <BarChart
              h={200}
              data={memoryChartData}
              dataKey="time"
              series={[{ name: "Memory (MB)", color: "grape.6" }]}
              gridAxis="xy"
              tickLine="xy"
            />
          )}
        </Card>

        {/* CPU time */}
        <Card withBorder padding="md">
          <Text fw={600} size="sm" mb="md">
            CPU Time (ms)
          </Text>
          {loading || series.length === 0 ? (
            <Box h={200}>
              {loading ? (
                <Skeleton height={200} radius="sm" />
              ) : (
                <Text c="dimmed" ta="center" pt={80} size="sm">
                  No data for this time range
                </Text>
              )}
            </Box>
          ) : (
            <AreaChart
              h={200}
              data={cpuChartData}
              dataKey="time"
              series={[{ name: "CPU (ms)", color: "orange.6" }]}
              curveType="monotone"
              withDots={false}
              gridAxis="xy"
              tickLine="xy"
              strokeWidth={2}
            />
          )}
        </Card>

        {/* Success rate by DAG */}
        <Card withBorder padding="md">
          <Text fw={600} size="sm" mb="md">
            Success Rate by DAG
          </Text>
          {loading || dagStats.length === 0 ? (
            <Box h={200}>
              {loading ? (
                <Skeleton height={200} radius="sm" />
              ) : (
                <Text c="dimmed" ta="center" pt={80} size="sm">
                  No data for this time range
                </Text>
              )}
            </Box>
          ) : (
            <Stack gap="xs" mt="xs">
              {dagStats.slice(0, 8).map((d) => (
                <div key={d.dag_id}>
                  <Group justify="space-between" mb={4}>
                    <Text size="xs" fw={500} truncate maw={180}>
                      {d.dag_id}
                    </Text>
                    <Text size="xs" c="dimmed">
                      {fmtPercent(d.success_rate)} ({d.count} runs)
                    </Text>
                  </Group>
                  <Progress
                    value={d.success_rate * 100}
                    color={
                      d.success_rate >= 0.95 ? "green" : d.success_rate >= 0.7 ? "yellow" : "red"
                    }
                    size="sm"
                    radius="sm"
                  />
                </div>
              ))}
            </Stack>
          )}
        </Card>
      </SimpleGrid>

      {/* Per-DAG Aggregate Table */}
      <Card withBorder padding="md">
        <Text fw={600} size="sm" mb="md">
          DAG Performance Summary
        </Text>
        {loading ? (
          <Skeleton height={120} radius="sm" />
        ) : dagStats.length === 0 ? (
          <Text c="dimmed" size="sm" ta="center" py="lg">
            No metrics recorded yet. Run some tasks to see data here.
          </Text>
        ) : (
          <Table striped highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>DAG</Table.Th>
                <Table.Th>Runs</Table.Th>
                <Table.Th>Avg Duration</Table.Th>
                <Table.Th>Max Duration</Table.Th>
                <Table.Th>Avg Memory</Table.Th>
                <Table.Th>Max Memory</Table.Th>
                <Table.Th>Success Rate</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {dagStats.map((d) => (
                <Table.Tr key={d.dag_id}>
                  <Table.Td>
                    <Anchor
                      size="sm"
                      fw={500}
                      onClick={() => router.push(`/dags?id=${d.dag_id}`)}
                      style={{ cursor: "pointer" }}
                    >
                      {d.dag_id}
                    </Anchor>
                  </Table.Td>
                  <Table.Td>
                    <Text size="sm">{d.count}</Text>
                  </Table.Td>
                  <Table.Td>
                    <Text size="sm">{fmtDuration(d.avg_duration_ms)}</Text>
                  </Table.Td>
                  <Table.Td>
                    <Text size="sm">{fmtDuration(d.max_duration_ms)}</Text>
                  </Table.Td>
                  <Table.Td>
                    <Text size="sm">{fmtMemory(d.avg_memory_bytes)}</Text>
                  </Table.Td>
                  <Table.Td>
                    <Text size="sm">{fmtMemory(d.max_memory_bytes)}</Text>
                  </Table.Td>
                  <Table.Td>
                    <Badge
                      color={
                        d.success_rate >= 0.95 ? "green" : d.success_rate >= 0.7 ? "yellow" : "red"
                      }
                      variant="light"
                      size="sm"
                    >
                      {fmtPercent(d.success_rate)}
                    </Badge>
                  </Table.Td>
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
        )}
      </Card>

      {/* Slowest Tasks Table */}
      <Card withBorder padding="md">
        <Group mb="md">
          <ThemeIcon color="red" variant="light" size={28} radius="md">
            <IconClock size={16} />
          </ThemeIcon>
          <Text fw={600} size="sm">
            Slowest Tasks
          </Text>
        </Group>
        {loading ? (
          <Skeleton height={120} radius="sm" />
        ) : slowestTasks.length === 0 ? (
          <Text c="dimmed" size="sm" ta="center" py="lg">
            No metrics recorded yet.
          </Text>
        ) : (
          <Table striped highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Task</Table.Th>
                <Table.Th>DAG</Table.Th>
                <Table.Th>Run</Table.Th>
                <Table.Th>Duration</Table.Th>
                <Table.Th>Peak Memory</Table.Th>
                <Table.Th>Exit Code</Table.Th>
                <Table.Th>Completed At</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {slowestTasks.map((t) => (
                <Table.Tr key={t.task_instance_id}>
                  <Table.Td>
                    <Text size="sm" fw={500}>
                      {t.task_id}
                    </Text>
                  </Table.Td>
                  <Table.Td>
                    <Anchor
                      size="sm"
                      onClick={() => router.push(`/dags?id=${t.dag_id}`)}
                      style={{ cursor: "pointer" }}
                    >
                      {t.dag_id}
                    </Anchor>
                  </Table.Td>
                  <Table.Td>
                    <Anchor
                      size="xs"
                      c="dimmed"
                      ff="monospace"
                      onClick={() => router.push(`/runs?id=${t.run_id}`)}
                      style={{ cursor: "pointer" }}
                    >
                      {t.run_id.slice(0, 20)}…
                    </Anchor>
                  </Table.Td>
                  <Table.Td>
                    <Badge color="red" variant="light" size="sm">
                      {fmtDuration(t.duration_ms)}
                    </Badge>
                  </Table.Td>
                  <Table.Td>
                    <Text size="sm">{fmtMemory(t.peak_memory_bytes)}</Text>
                  </Table.Td>
                  <Table.Td>
                    <Badge color={t.exit_code === 0 ? "green" : "red"} variant="light" size="sm">
                      {t.exit_code}
                    </Badge>
                  </Table.Td>
                  <Table.Td>
                    <Text size="xs" c="dimmed">
                      {new Date(t.created_at).toLocaleString()}
                    </Text>
                  </Table.Td>
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
        )}
      </Card>

      {/* Per-task executor type breakdown */}
      {!loading && series.length > 0 && (
        <Card withBorder padding="md">
          <Text fw={600} size="sm" mb="md">
            Recent Task Executions
          </Text>
          <Table striped highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Task</Table.Th>
                <Table.Th>Run</Table.Th>
                <Table.Th>Duration</Table.Th>
                <Table.Th>Memory</Table.Th>
                <Table.Th>CPU</Table.Th>
                <Table.Th>Status</Table.Th>
                <Table.Th>Time</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {series
                .slice()
                .reverse()
                .slice(0, 50)
                .map((p, i) => (
                  <Table.Tr key={i}>
                    <Table.Td>
                      <Text size="sm" fw={500}>
                        {p.task_id}
                      </Text>
                    </Table.Td>
                    <Table.Td>
                      <Anchor
                        size="xs"
                        c="dimmed"
                        ff="monospace"
                        onClick={() => router.push(`/runs?id=${p.run_id}`)}
                        style={{ cursor: "pointer" }}
                      >
                        {p.run_id.slice(0, 18)}…
                      </Anchor>
                    </Table.Td>
                    <Table.Td>
                      <Text size="sm">{fmtDuration(p.duration_ms)}</Text>
                    </Table.Td>
                    <Table.Td>
                      <Text size="sm">{fmtMemory(p.memory_bytes)}</Text>
                    </Table.Td>
                    <Table.Td>
                      <Text size="sm">{fmtDuration(p.cpu_ms)}</Text>
                    </Table.Td>
                    <Table.Td>
                      <Badge
                        color={
                          p.status === "success" ? "green" : p.status === "failed" ? "red" : "gray"
                        }
                        variant="light"
                        size="xs"
                      >
                        {p.status || "—"}
                      </Badge>
                    </Table.Td>
                    <Table.Td>
                      <Text size="xs" c="dimmed">
                        {new Date(p.timestamp).toLocaleTimeString()}
                      </Text>
                    </Table.Td>
                  </Table.Tr>
                ))}
            </Table.Tbody>
          </Table>
        </Card>
      )}
    </Stack>
  );
}
