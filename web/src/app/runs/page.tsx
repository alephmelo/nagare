"use client";

import { useEffect, useState, useRef, Suspense, useCallback } from "react";
import { useSearchParams, useRouter } from "next/navigation";
import { apiFetch } from "../../lib/apiFetch";
import {
  Title,
  Card,
  Code,
  Badge,
  Button,
  Group,
  Text,
  Loader,
  Center,
  ActionIcon,
  Collapse,
  Stack,
  Divider,
  Skeleton,
  ThemeIcon,
  Tooltip,
  Box,
  Tabs,
  Table,
  Menu,
  UnstyledButton,
  Pagination,
} from "@mantine/core";
import {
  IconArrowLeft,
  IconRefresh,
  IconCheck,
  IconX,
  IconClock,
  IconChevronDown,
  IconChevronRight,
  IconTerminal2,
  IconAlertCircle,
  IconPlayerStop,
  IconPlayerPlay,
  IconFilter,
  IconUser,
  IconRobot,
  IconActivity,
} from "@tabler/icons-react";
import { notifications } from "@mantine/notifications";

interface RunTask {
  ID: string;
  TaskID: string;
  Status: string;
  Output: string;
  Attempt: number;
  CreatedAt: string;
  UpdatedAt: string;
  Command?: string;
  Metrics?: {
    DurationMs: number;
    CpuUserMs: number;
    CpuSystemMs: number;
    PeakMemoryBytes: number;
    ExitCode: number;
    ExecutorType: string;
  };
}

interface Run {
  ID: string;
  DAGID: string;
  Status: string;
  ExecDate: string;
  TriggerType: string;
  CreatedAt: string;
  CompletedAt?: string;
}

function StatusIcon({ status }: { status: string }) {
  switch (status) {
    case "success":
      return (
        <ThemeIcon color="green" variant="light" size="md" radius="xl">
          <IconCheck size={14} />
        </ThemeIcon>
      );
    case "failed":
      return (
        <ThemeIcon color="red" variant="light" size="md" radius="xl">
          <IconX size={14} />
        </ThemeIcon>
      );
    case "running":
      return (
        <ThemeIcon color="blue" variant="light" size="md" radius="xl">
          <Loader size={12} color="blue" />
        </ThemeIcon>
      );
    case "queued":
      return (
        <ThemeIcon color="yellow" variant="light" size="md" radius="xl">
          <IconClock size={14} />
        </ThemeIcon>
      );
    case "up_for_retry":
      return (
        <ThemeIcon color="orange" variant="light" size="md" radius="xl">
          <IconClock size={14} />
        </ThemeIcon>
      );
    case "cancelled":
      return (
        <ThemeIcon color="gray" variant="light" size="md" radius="xl">
          <IconPlayerStop size={14} />
        </ThemeIcon>
      );
    default:
      return (
        <ThemeIcon color="gray" variant="light" size="md" radius="xl">
          <IconAlertCircle size={14} />
        </ThemeIcon>
      );
  }
}

// useSSELogs subscribes to the SSE log stream for a task while it is running.
// Returns the accumulated live log string (empty string when not streaming).
function useSSELogs(taskInstanceID: string, runID: string, active: boolean): string {
  const [lines, setLines] = useState<string[]>([]);
  const esRef = useRef<EventSource | null>(null);
  const retryRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    if (!active) {
      if (retryRef.current) clearTimeout(retryRef.current);
      if (esRef.current) {
        esRef.current.close();
        esRef.current = null;
      }
      setLines([]);
      return;
    }

    function connect() {
      if (esRef.current) return; // already connected
      const storedKey = localStorage.getItem("nagare_api_key");
      const params = storedKey ? `?token=${encodeURIComponent(storedKey)}` : "";
      const url = `/api/runs/${runID}/tasks/${taskInstanceID}/logs${params}`;
      const es = new EventSource(url);
      esRef.current = es;

      es.onmessage = (evt) => {
        setLines((prev) => [...prev, evt.data]);
      };

      es.onerror = () => {
        es.close();
        esRef.current = null;
        // Retry after 1 s — the task may still be running.
        retryRef.current = setTimeout(connect, 1000);
      };
    }

    connect();

    return () => {
      if (retryRef.current) clearTimeout(retryRef.current);
      if (esRef.current) {
        esRef.current.close();
        esRef.current = null;
      }
    };
  }, [taskInstanceID, runID, active]);

  return lines.join("\n");
}

function TaskRow({
  task,
  runID,
  onRetry,
  onKill,
}: {
  task: RunTask;
  runID: string;
  onRetry: (taskID: string) => void;
  onKill: (taskID: string) => void;
}) {
  const isLive = task.Status === "running" || task.Status === "queued";
  // Auto-expand running tasks so the live log stream starts immediately.
  // Failed/retry tasks also start expanded to surface errors.
  const [expanded, setExpanded] = useState(
    isLive || task.Status === "failed" || task.Status === "up_for_retry"
  );
  const [attempts, setAttempts] = useState<RunTask[]>([]);
  const [loadingAttempts, setLoadingAttempts] = useState(false);
  const liveOutput = useSSELogs(task.ID, runID, isLive && expanded);
  const displayOutput = isLive ? liveOutput : task.Output;
  const hasOutput = displayOutput && displayOutput.trim().length > 0;
  const hasMultipleAttempts = task.Attempt > 1;

  const fetchAttempts = async () => {
    if (attempts.length > 0 || task.Attempt <= 1) return;
    setLoadingAttempts(true);
    try {
      const res = await apiFetch(`/api/runs/${runID}/tasks/${task.TaskID}/attempts`);
      if (res.ok) setAttempts(await res.json());
    } catch {
      /* noop */
    } finally {
      setLoadingAttempts(false);
    }
  };

  const handleExpand = () => {
    if (isLive || hasOutput || hasMultipleAttempts) {
      const next = !expanded;
      setExpanded(next);
      if (next) fetchAttempts();
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case "success":
        return "green";
      case "failed":
        return "red";
      case "running":
        return "blue";
      case "queued":
        return "yellow";
      case "up_for_retry":
        return "orange";
      case "cancelled":
        return "gray";
      default:
        return "gray";
    }
  };

  return (
    <Card
      padding="0"
      mb="xs"
      style={{
        border:
          task.Status === "failed"
            ? "1px solid var(--mantine-color-red-3)"
            : task.Status === "up_for_retry"
              ? "1px solid var(--mantine-color-orange-3)"
              : "1px solid var(--mantine-color-default-border)",
      }}
    >
      <Group
        px="md"
        py="sm"
        justify="space-between"
        style={{ cursor: isLive || hasOutput || hasMultipleAttempts ? "pointer" : "default" }}
        onClick={handleExpand}
      >
        <Group gap="sm">
          <StatusIcon status={task.Status} />
          <div>
            <Group gap="xs">
              <Text fw={600} size="sm">
                {task.TaskID}
              </Text>
              {hasMultipleAttempts && (
                <Badge size="xs" variant="dot" color="orange">
                  Attempt #{task.Attempt}
                </Badge>
              )}
              {task.Metrics && task.Metrics.DurationMs > 0 && (
                <Badge size="xs" variant="outline" color="gray">
                  {task.Metrics.DurationMs >= 1000
                    ? `${(task.Metrics.DurationMs / 1000).toFixed(1)}s`
                    : `${task.Metrics.DurationMs}ms`}
                </Badge>
              )}
              {task.Metrics && task.Metrics.PeakMemoryBytes > 0 && (
                <Badge size="xs" variant="outline" color="cyan">
                  {task.Metrics.PeakMemoryBytes >= 1024 * 1024 * 1024
                    ? `${(task.Metrics.PeakMemoryBytes / (1024 * 1024 * 1024)).toFixed(1)} GB`
                    : task.Metrics.PeakMemoryBytes >= 1024 * 1024
                      ? `${(task.Metrics.PeakMemoryBytes / (1024 * 1024)).toFixed(1)} MB`
                      : `${(task.Metrics.PeakMemoryBytes / 1024).toFixed(0)} KB`}
                </Badge>
              )}
            </Group>
            <Text size="xs" c="dimmed">
              Last Updated {new Date(task.UpdatedAt).toLocaleTimeString()}
            </Text>
          </div>
        </Group>
        <Group gap="sm">
          <Badge color={getStatusColor(task.Status)} variant="light" radius="sm" size="sm">
            {task.Status.toUpperCase()}
          </Badge>
          {(task.Status === "success" ||
            task.Status === "failed" ||
            task.Status === "cancelled") && (
            <Tooltip label="Retry Task">
              <ActionIcon
                variant="light"
                color="blue"
                size="sm"
                onClick={(e) => {
                  e.stopPropagation();
                  onRetry(task.TaskID);
                }}
              >
                <IconPlayerPlay size={12} />
              </ActionIcon>
            </Tooltip>
          )}
          {task.Status === "running" && (
            <Tooltip label="Kill Task">
              <ActionIcon
                variant="light"
                color="red"
                size="sm"
                onClick={(e) => {
                  e.stopPropagation();
                  onKill(task.TaskID);
                }}
              >
                <IconPlayerStop size={12} />
              </ActionIcon>
            </Tooltip>
          )}
          {(isLive || hasOutput || hasMultipleAttempts) && (
            <ActionIcon variant="transparent" size="sm" color="dimmed">
              {expanded ? <IconChevronDown size={16} /> : <IconChevronRight size={16} />}
            </ActionIcon>
          )}
        </Group>
      </Group>

      <Collapse in={expanded}>
        <Divider />
        {loadingAttempts ? (
          <Box p="md">
            <Loader size="xs" />
          </Box>
        ) : hasMultipleAttempts && attempts.length > 0 ? (
          <Tabs
            defaultValue={String(attempts[attempts.length - 1].Attempt)}
            style={{ backgroundColor: "var(--log-bg)" }}
          >
            <Tabs.List px="md" pt="xs">
              {attempts.map((a) => (
                <Tabs.Tab
                  key={a.Attempt}
                  value={String(a.Attempt)}
                  leftSection={<StatusIcon status={a.Status} />}
                >
                  <Text size="xs" fw={600}>
                    Attempt #{a.Attempt}
                  </Text>
                </Tabs.Tab>
              ))}
            </Tabs.List>
            {attempts.map((a) => (
              <Tabs.Panel key={a.Attempt} value={String(a.Attempt)} p="md">
                <Text size="xs" c="dimmed" mb="xs">
                  {new Date(a.UpdatedAt).toLocaleString()}
                </Text>

                {a.Command && (
                  <Box mb="md">
                    <Group gap="xs" mb="xs">
                      <IconTerminal2 size={14} color="var(--mantine-color-dimmed)" />
                      <Text size="xs" c="dimmed" tt="uppercase" fw={700}>
                        Command
                      </Text>
                    </Group>
                    <Code block>{a.Command}</Code>
                  </Box>
                )}

                <Group gap="xs" mb="xs">
                  <IconTerminal2 size={14} color="var(--mantine-color-dimmed)" />
                  <Text size="xs" c="dimmed" tt="uppercase" fw={700}>
                    Output Log
                  </Text>
                </Group>

                <Code
                  block
                  style={{
                    whiteSpace: "pre-wrap",
                    maxHeight: "280px",
                    overflowY: "auto",
                    fontSize: "12px",
                    lineHeight: 1.7,
                    backgroundColor: "transparent",
                    border: "none",
                    color:
                      a.Status === "failed" ? "var(--log-text-failed)" : "var(--log-text-default)",
                  }}
                >
                  {a.Output || "No output for this attempt."}
                </Code>
              </Tabs.Panel>
            ))}
          </Tabs>
        ) : (
          <Box p="md" style={{ backgroundColor: "var(--log-bg)" }}>
            {task.Command && (
              <Box mb="md">
                <Group gap="xs" mb="xs">
                  <IconTerminal2 size={14} color="var(--mantine-color-dimmed)" />
                  <Text size="xs" c="dimmed" tt="uppercase" fw={700}>
                    Command
                  </Text>
                </Group>
                <Code block>{task.Command}</Code>
              </Box>
            )}

            <Group gap="xs" mb="xs">
              <IconTerminal2 size={14} color="var(--mantine-color-dimmed)" />
              <Text size="xs" c="dimmed" tt="uppercase" fw={700}>
                Output Log
                {isLive && (
                  <>
                    {" "}
                    &mdash;{" "}
                    <Text span size="xs" c="blue" fw={400}>
                      streaming live
                    </Text>
                  </>
                )}
              </Text>
            </Group>
            <Code
              block
              style={{
                whiteSpace: "pre-wrap",
                maxHeight: "300px",
                overflowY: "auto",
                fontSize: "12px",
                lineHeight: 1.7,
                backgroundColor: "transparent",
                border: "none",
                color:
                  task.Status === "failed" ? "var(--log-text-failed)" : "var(--log-text-default)",
              }}
            >
              {displayOutput || (isLive ? "Waiting for output..." : "No output generated yet.")}
            </Code>
          </Box>
        )}
      </Collapse>
    </Card>
  );
}

function RunDetailsContent() {
  const searchParams = useSearchParams();
  const id = searchParams.get("id");
  const router = useRouter();
  const [tasks, setTasks] = useState<RunTask[]>([]);
  const [run, setRun] = useState<Run | null>(null);
  const [loading, setLoading] = useState(true);

  const fetchTasks = useCallback(async () => {
    if (!id) return;
    try {
      const [tasksRes, runsRes] = await Promise.all([
        apiFetch(`/api/runs/${id}/tasks`),
        apiFetch(`/api/runs?page=1&limit=100`),
      ]);
      if (tasksRes.ok) setTasks(await tasksRes.json());
      if (runsRes.ok) {
        const runsData = await runsRes.json();
        const matchedRun = (runsData.data || []).find((r: Run) => r.ID === id);
        if (matchedRun) setRun(matchedRun);
      }
    } catch (err) {
      console.error("Failed to fetch tasks", err);
    } finally {
      setLoading(false);
    }
  }, [id]);

  useEffect(() => {
    fetchTasks();
    const interval = setInterval(fetchTasks, 3000);
    return () => clearInterval(interval);
  }, [fetchTasks]);

  const handleRetry = async (taskID: string) => {
    try {
      const res = await apiFetch(`/api/runs/${id}/tasks/${taskID}/retry`, { method: "POST" });
      if (res.ok) {
        fetchTasks();
        notifications.show({
          title: "Task Requeued",
          message: `Sent ${taskID} back to pending.`,
          color: "green",
        });
      } else {
        notifications.show({
          title: "Retry Failed",
          message: `Could not retry ${taskID}.`,
          color: "red",
        });
      }
    } catch {
      notifications.show({
        title: "Network Error",
        message: "Could not communicate with the API.",
        color: "red",
      });
    }
  };

  const handleKillTask = async (taskID: string) => {
    try {
      const res = await apiFetch(`/api/runs/${id}/tasks/${taskID}/kill`, { method: "POST" });
      if (res.ok) {
        fetchTasks();
        notifications.show({
          title: "Task Terminated",
          message: `Termination signal sent to ${taskID}.`,
          color: "orange",
        });
      }
    } catch {
      /* noop */
    }
  };

  const handleKillRun = async () => {
    try {
      const res = await apiFetch(`/api/runs/${id}/kill`, { method: "POST" });
      if (res.ok) {
        fetchTasks();
        notifications.show({
          title: "Run Terminated",
          message: `Termination signal sent to run ${id}.`,
          color: "orange",
        });
      }
    } catch {
      /* noop */
    }
  };

  const getRunStatusColor = (status: string) => {
    switch (status) {
      case "success":
        return "green";
      case "failed":
        return "red";
      case "running":
        return "blue";
      case "cancelled":
        return "gray";
      default:
        return "gray";
    }
  };

  const elapsedTime = run
    ? run.CompletedAt
      ? `${Math.max(1, Math.floor((new Date(run.CompletedAt).getTime() - new Date(run.CreatedAt).getTime()) / 1000))}s`
      : run.Status === "running"
        ? `${Math.max(1, Math.floor((new Date().getTime() - new Date(run.CreatedAt).getTime()) / 1000))}s elapsed`
        : "—"
    : null;

  const successCount = tasks.filter((t) => t.Status === "success").length;
  const failedCount = tasks.filter((t) => t.Status === "failed").length;
  const runningCount = tasks.filter((t) => t.Status === "running").length;
  const retryCount = tasks.filter((t) => t.Status === "up_for_retry").length;

  if (!id) {
    return (
      <Center h={200}>
        <Title order={3} c="dimmed">
          No Run ID provided
        </Title>
      </Center>
    );
  }

  return (
    <>
      <Group justify="space-between" mb="xl">
        <Group>
          <Button
            variant="subtle"
            color="gray"
            leftSection={<IconArrowLeft size={16} />}
            onClick={() => router.push("/runs")}
          >
            Back
          </Button>
          <div>
            <Group gap="xs" align="center">
              <Title order={3}>Run Details</Title>
              {run && (
                <Badge size="sm" color={getRunStatusColor(run.Status)} variant="light">
                  {run.Status.toUpperCase()}
                </Badge>
              )}
            </Group>
            <Text size="xs" c="dimmed" style={{ fontFamily: "monospace" }}>
              {id}
            </Text>
          </div>
        </Group>
        <Group gap="sm">
          {run && run.Status === "running" && (
            <Button
              variant="light"
              color="red"
              leftSection={<IconPlayerStop size={16} />}
              onClick={handleKillRun}
            >
              Kill Run
            </Button>
          )}
          <Button
            variant="light"
            leftSection={<IconRefresh size={16} />}
            onClick={fetchTasks}
            loading={loading}
          >
            Refresh
          </Button>
        </Group>
      </Group>

      {loading && !run ? (
        <Skeleton height={72} mb="xl" radius="md" />
      ) : (
        run && (
          <Card padding="md" mb="xl">
            <Group grow>
              <div>
                <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                  Pipeline
                </Text>
                <Text
                  fw={600}
                  size="sm"
                  mt={4}
                  style={{
                    cursor: "pointer",
                    textDecoration: "underline",
                    textUnderlineOffset: "3px",
                  }}
                  onClick={() => router.push(`/dags?id=${run.DAGID}`)}
                >
                  {run.DAGID}
                </Text>
              </div>
              <div>
                <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                  Trigger
                </Text>
                <Text fw={600} size="sm" mt={4}>
                  {run.TriggerType === "manual"
                    ? "Manual"
                    : run.TriggerType === "scheduled"
                      ? "Scheduled"
                      : "Triggered"}
                </Text>
              </div>
              <div>
                <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                  Started At
                </Text>
                <Text fw={600} size="sm" mt={4}>
                  {new Date(run.CreatedAt).toLocaleString()}
                </Text>
              </div>
              <div>
                <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                  Duration
                </Text>
                <Text fw={600} size="sm" mt={4}>
                  {elapsedTime ?? "—"}
                </Text>
              </div>
              <div>
                <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                  Tasks
                </Text>
                <Group gap={4} mt={4}>
                  {successCount > 0 && (
                    <Badge size="xs" color="green" variant="light">
                      {successCount} ok
                    </Badge>
                  )}
                  {failedCount > 0 && (
                    <Badge size="xs" color="red" variant="light">
                      {failedCount} failed
                    </Badge>
                  )}
                  {runningCount > 0 && (
                    <Badge size="xs" color="blue" variant="light">
                      {runningCount} running
                    </Badge>
                  )}
                  {retryCount > 0 && (
                    <Badge size="xs" color="orange" variant="light">
                      {retryCount} retry
                    </Badge>
                  )}
                  {tasks.length === 0 && (
                    <Text size="xs" c="dimmed">
                      —
                    </Text>
                  )}
                </Group>
              </div>
            </Group>
          </Card>
        )
      )}

      <Title order={4} mb="md" c="dimmed">
        Task Execution Log
      </Title>
      {loading && tasks.length === 0 ? (
        <Stack gap="xs">
          <Skeleton height={56} radius="md" />
          <Skeleton height={56} radius="md" />
          <Skeleton height={56} radius="md" />
        </Stack>
      ) : tasks.length === 0 ? (
        <Card padding="xl">
          <Center>
            <Text c="dimmed">No tasks found for this run.</Text>
          </Center>
        </Card>
      ) : (
        <Stack gap="xs">
          {tasks.map((task) => (
            <TaskRow
              key={task.ID}
              task={task}
              runID={id}
              onRetry={handleRetry}
              onKill={handleKillTask}
            />
          ))}
        </Stack>
      )}
    </>
  );
}

// ---------------------------------------------------------------------------
// Runs List View (no ?id param)
// ---------------------------------------------------------------------------
function RunListContent() {
  const [runs, setRuns] = useState<Run[]>([]);
  const [dags, setDags] = useState<{ ID: string }[]>([]);
  const [totalRuns, setTotalRuns] = useState(0);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [dagFilter, setDagFilter] = useState<string | null>("all");
  const [statusFilter, setStatusFilter] = useState<string | null>("all");
  const [triggerFilter, setTriggerFilter] = useState<string | null>("all");
  const limit = 20;
  const router = useRouter();

  const getStatusColor = (status: string) => {
    switch (status) {
      case "success":
        return "green";
      case "failed":
        return "red";
      case "running":
        return "blue";
      case "queued":
        return "yellow";
      case "cancelled":
        return "gray";
      default:
        return "gray";
    }
  };

  const fetchData = useCallback(async () => {
    try {
      setLoading(true);
      const [runsRes, dagsRes] = await Promise.all([
        apiFetch(
          `/api/runs?page=${page}&limit=${limit}&dag_id=${dagFilter || "all"}&status=${statusFilter || "all"}&trigger=${triggerFilter || "all"}`
        ),
        apiFetch("/api/dags"),
      ]);
      if (runsRes.ok) {
        const data = await runsRes.json();
        setRuns(data.data || []);
        setTotalRuns(data.total || 0);
      }
      if (dagsRes.ok) setDags(await dagsRes.json());
    } catch (err) {
      console.error("Failed to fetch runs", err);
    } finally {
      setLoading(false);
    }
  }, [page, dagFilter, statusFilter, triggerFilter]);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, [fetchData]);

  const handleKillRun = async (e: React.MouseEvent, runID: string) => {
    e.stopPropagation();
    try {
      const res = await apiFetch(`/api/runs/${runID}/kill`, { method: "POST" });
      if (res.ok) {
        notifications.show({
          title: "Run Terminated",
          message: `Termination signal sent to run ${runID}.`,
          color: "orange",
        });
        fetchData();
      }
    } catch (err) {
      console.error("Failed to kill run:", err);
    }
  };

  return (
    <>
      <Group justify="space-between" mb="xl">
        <Title order={2}>Runs</Title>
        <Button leftSection={<IconRefresh size={16} />} variant="light" onClick={fetchData}>
          Refresh
        </Button>
      </Group>

      <Card padding="0">
        <Table.ScrollContainer minWidth={800}>
          <Table verticalSpacing="md" horizontalSpacing="md" striped highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}>
                  <Text size="sm" fw={700}>
                    Run ID
                  </Text>
                </Table.Th>
                <Table.Th style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}>
                  <Menu shadow="md" width={200}>
                    <Menu.Target>
                      <UnstyledButton>
                        <Group gap={4}>
                          <Text size="sm" fw={700} c={dagFilter !== "all" ? "blue" : undefined}>
                            DAG
                          </Text>
                          <IconFilter
                            size={14}
                            color={
                              dagFilter !== "all"
                                ? "var(--mantine-color-blue-filled)"
                                : "var(--mantine-color-gray-5)"
                            }
                          />
                        </Group>
                      </UnstyledButton>
                    </Menu.Target>
                    <Menu.Dropdown>
                      <Menu.Label>Filter by DAG</Menu.Label>
                      <Menu.Item
                        onClick={() => {
                          setDagFilter("all");
                          setPage(1);
                        }}
                        leftSection={
                          dagFilter === "all" ? (
                            <IconCheck size={14} />
                          ) : (
                            <div style={{ width: 14 }} />
                          )
                        }
                      >
                        All DAGs
                      </Menu.Item>
                      {dags.map((d) => (
                        <Menu.Item
                          key={d.ID}
                          onClick={() => {
                            setDagFilter(d.ID);
                            setPage(1);
                          }}
                          leftSection={
                            dagFilter === d.ID ? (
                              <IconCheck size={14} />
                            ) : (
                              <div style={{ width: 14 }} />
                            )
                          }
                        >
                          {d.ID}
                        </Menu.Item>
                      ))}
                    </Menu.Dropdown>
                  </Menu>
                </Table.Th>
                <Table.Th style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}>
                  <Menu shadow="md" width={150}>
                    <Menu.Target>
                      <UnstyledButton>
                        <Group gap={4}>
                          <Text size="sm" fw={700} c={statusFilter !== "all" ? "blue" : undefined}>
                            Status
                          </Text>
                          <IconFilter
                            size={14}
                            color={
                              statusFilter !== "all"
                                ? "var(--mantine-color-blue-filled)"
                                : "var(--mantine-color-gray-5)"
                            }
                          />
                        </Group>
                      </UnstyledButton>
                    </Menu.Target>
                    <Menu.Dropdown>
                      <Menu.Label>Filter by Status</Menu.Label>
                      {[
                        { value: "all", label: "All Statuses" },
                        { value: "success", label: "Success" },
                        { value: "failed", label: "Failed" },
                        { value: "cancelled", label: "Cancelled" },
                        { value: "running", label: "Running" },
                      ].map((opt) => (
                        <Menu.Item
                          key={opt.value}
                          onClick={() => {
                            setStatusFilter(opt.value);
                            setPage(1);
                          }}
                          leftSection={
                            statusFilter === opt.value ? (
                              <IconCheck size={14} />
                            ) : (
                              <div style={{ width: 14 }} />
                            )
                          }
                        >
                          {opt.label}
                        </Menu.Item>
                      ))}
                    </Menu.Dropdown>
                  </Menu>
                </Table.Th>
                <Table.Th style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}>
                  <Text size="sm" fw={700}>
                    Execution Date
                  </Text>
                </Table.Th>
                <Table.Th style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}>
                  <Menu shadow="md" width={150}>
                    <Menu.Target>
                      <UnstyledButton>
                        <Group gap={4}>
                          <Text size="sm" fw={700} c={triggerFilter !== "all" ? "blue" : undefined}>
                            Trigger
                          </Text>
                          <IconFilter
                            size={14}
                            color={
                              triggerFilter !== "all"
                                ? "var(--mantine-color-blue-filled)"
                                : "var(--mantine-color-gray-5)"
                            }
                          />
                        </Group>
                      </UnstyledButton>
                    </Menu.Target>
                    <Menu.Dropdown>
                      <Menu.Label>Filter by Trigger</Menu.Label>
                      {[
                        { value: "all", label: "All Triggers" },
                        { value: "manual", label: "Manual" },
                        { value: "scheduled", label: "Scheduled" },
                        { value: "triggered", label: "Triggered" },
                      ].map((opt) => (
                        <Menu.Item
                          key={opt.value}
                          onClick={() => {
                            setTriggerFilter(opt.value);
                            setPage(1);
                          }}
                          leftSection={
                            triggerFilter === opt.value ? (
                              <IconCheck size={14} />
                            ) : (
                              <div style={{ width: 14 }} />
                            )
                          }
                        >
                          {opt.label}
                        </Menu.Item>
                      ))}
                    </Menu.Dropdown>
                  </Menu>
                </Table.Th>
                <Table.Th style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}>
                  <Text size="sm" fw={700}>
                    Elapsed
                  </Text>
                </Table.Th>
                <Table.Th style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}>
                  <Text size="sm" fw={700}>
                    Actions
                  </Text>
                </Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {runs.map((run) => (
                <Table.Tr
                  key={run.ID}
                  onClick={() => router.push(`/runs?id=${run.ID}`)}
                  style={{ cursor: "pointer" }}
                >
                  <Table.Td>
                    <Text size="sm" fw={500}>
                      {run.ID}
                    </Text>
                  </Table.Td>
                  <Table.Td>
                    <Badge variant="outline" color="gray">
                      {run.DAGID}
                    </Badge>
                  </Table.Td>
                  <Table.Td>
                    <Badge color={getStatusColor(run.Status)} variant="light" size="sm" radius="sm">
                      {run.Status.toUpperCase()}
                    </Badge>
                  </Table.Td>
                  <Table.Td>
                    <Text size="sm">{new Date(run.ExecDate).toLocaleString()}</Text>
                  </Table.Td>
                  <Table.Td>
                    {run.TriggerType === "manual" ? (
                      <Badge
                        variant="light"
                        color="blue"
                        size="sm"
                        leftSection={
                          <IconUser
                            size={12}
                            style={{ display: "flex", alignItems: "center", marginTop: "2px" }}
                          />
                        }
                      >
                        Manual
                      </Badge>
                    ) : run.TriggerType === "scheduled" ? (
                      <Badge
                        variant="light"
                        color="teal"
                        size="sm"
                        leftSection={
                          <IconRobot
                            size={12}
                            style={{ display: "flex", alignItems: "center", marginTop: "2px" }}
                          />
                        }
                      >
                        Scheduled
                      </Badge>
                    ) : (
                      <Badge
                        variant="light"
                        color="violet"
                        size="sm"
                        leftSection={
                          <IconActivity
                            size={12}
                            style={{ display: "flex", alignItems: "center", marginTop: "2px" }}
                          />
                        }
                      >
                        Triggered
                      </Badge>
                    )}
                  </Table.Td>
                  <Table.Td>
                    <Text size="sm" c="dimmed">
                      {run.CompletedAt
                        ? `${Math.max(1, Math.floor((new Date(run.CompletedAt).getTime() - new Date(run.CreatedAt).getTime()) / 1000))}s`
                        : run.Status === "running"
                          ? `${Math.max(1, Math.floor((new Date().getTime() - new Date(run.CreatedAt).getTime()) / 1000))}s`
                          : "—"}
                    </Text>
                  </Table.Td>
                  <Table.Td>
                    {run.Status === "running" && (
                      <Tooltip label="Kill Run">
                        <ActionIcon
                          variant="light"
                          color="red"
                          onClick={(e) => handleKillRun(e, run.ID)}
                          size="sm"
                        >
                          <IconPlayerStop size={14} />
                        </ActionIcon>
                      </Tooltip>
                    )}
                  </Table.Td>
                </Table.Tr>
              ))}
              {runs.length === 0 && !loading && (
                <Table.Tr>
                  <Table.Td colSpan={7} align="center" py="xl">
                    <Text c="dimmed">No runs found for this configuration.</Text>
                  </Table.Td>
                </Table.Tr>
              )}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
        {totalRuns > limit && (
          <Group
            justify="center"
            p="md"
            style={{ borderTop: "1px solid var(--mantine-color-default-border)" }}
          >
            <Pagination
              total={Math.ceil(totalRuns / limit)}
              value={page}
              onChange={setPage}
              color="cyan"
              withEdges
            />
          </Group>
        )}
      </Card>
    </>
  );
}

// ---------------------------------------------------------------------------
// Root export: switch between list and detail based on ?id param
// ---------------------------------------------------------------------------
function RunsPageContent() {
  const searchParams = useSearchParams();
  const id = searchParams.get("id");
  return id ? <RunDetailsContent /> : <RunListContent />;
}

export default function RunDetails() {
  return (
    <Suspense
      fallback={
        <Center h={200}>
          <Loader color="blue" />
        </Center>
      }
    >
      <RunsPageContent />
    </Suspense>
  );
}
