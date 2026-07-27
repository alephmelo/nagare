"use client";

import { useEffect, useState, useRef, Suspense, useCallback, useMemo } from "react";
import { useSearchParams, useRouter } from "next/navigation";
import { apiFetch } from "../../lib/apiFetch";
import { useVisibilityPoll } from "../../lib/useVisibilityPoll";
import {
  Card,
  Tooltip,
  Box,
  Tabs,
  Group,
  Badge,
  ActionIcon,
  Collapse,
  Divider,
  Loader,
  Skeleton,
  Center,
  Button,
  Text,
  Stack,
  Progress,
  Title,
} from "@mantine/core";
import {
  IconRefresh,
  IconChevronDown,
  IconChevronRight,
  IconPlayerStop,
  IconPlayerPlay,
} from "@tabler/icons-react";
import { notifications } from "@mantine/notifications";
import {
  ReactFlow,
  Background,
  useNodesState,
  useEdgesState,
  Position,
  MarkerType,
  Node,
  Edge,
  Handle,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { PageHeader } from "../../components/ui/PageHeader";
import { layoutTopologyStages } from "../../components/executionTopologyLayout";
import {
  fetchRunInspection,
  inspectionAttemptDuration,
  inspectionAttemptLabel,
  inspectionCurrentTasks,
  inspectionLogStreamURL,
  projectInspectionTopology,
  segmentInspectionTasks,
  summarizeInspectionTasks,
  InspectionCurrentTask,
  InspectionLogs,
  RunInspection,
} from "../../lib/runInspection";
import { StatusIcon } from "../../components/ui/StatusIcon";
import { getStatusColor, getStatusLabel } from "../../components/ui/StatusBadge";
import { LogTerminal } from "../../components/blocks/LogTerminal";
import { RunsTable, Run } from "../../components/blocks/RunsTable";

// Formats elapsed time into a human-readable string
function formatElapsed(seconds: number): string {
  if (seconds >= 3600) {
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    return `${h}h ${m}m`;
  }
  if (seconds >= 60) {
    const m = Math.floor(seconds / 60);
    const s = seconds % 60;
    return `${m}m ${s}s`;
  }
  return `${seconds}s`;
}

function formatDurationMs(durationMs: number): string {
  return durationMs >= 1000 ? `${(durationMs / 1000).toFixed(1)}s` : `${durationMs}ms`;
}

// Live ticking elapsed timer for running tasks/runs
function LiveElapsed({ startedAt }: { startedAt: string }) {
  const [elapsed, setElapsed] = useState("");

  useEffect(() => {
    const start = new Date(startedAt).getTime();
    const update = () => {
      const seconds = Math.max(1, Math.floor((Date.now() - start) / 1000));
      setElapsed(formatElapsed(seconds));
    };
    update();
    const id = setInterval(update, 1000);
    return () => clearInterval(id);
  }, [startedAt]);

  return (
    <Text size="sm" c="dimmed" style={{ animation: "liveTick 2s ease-in-out infinite" }}>
      {elapsed}
    </Text>
  );
}

// ---------------------------------------------------------------------------
// Run DAG Graph — shows task dependency relationships with live status
// ---------------------------------------------------------------------------

const RUN_NODE_W = 200;
const RUN_NODE_H = 44;
const TERMINAL_STATUSES = new Set(["success", "failed", "cancelled"]);

function statusBorderColor(status: string): string {
  switch (status) {
    case "success":
      return "var(--mantine-color-green-filled)";
    case "failed":
      return "var(--mantine-color-red-filled)";
    case "running":
      return "var(--mantine-color-blue-filled)";
    case "queued":
    case "pending":
      return "var(--mantine-color-gray-filled)";
    case "up_for_retry":
      return "var(--mantine-color-orange-filled)";
    case "cancelled":
      return "var(--mantine-color-yellow-filled)";
    default:
      return "var(--mantine-color-default-border)";
  }
}

function RunNodeComponent({
  data,
}: {
  data: { label: string; status: string; duration?: number };
}) {
  const borderColor = statusBorderColor(data.status);
  return (
    <div
      style={{
        background: "var(--node-bg)",
        color: "var(--node-text)",
        border: `2px solid ${borderColor}`,
        borderRadius: 8,
        padding: "6px 12px",
        fontSize: 12,
        fontWeight: 600,
        display: "flex",
        alignItems: "center",
        gap: 6,
        width: "100%",
        minWidth: 0,
      }}
    >
      <Handle
        type="target"
        position={Position.Top}
        style={{ visibility: "hidden", pointerEvents: "none" }}
      />
      <StatusIcon status={data.status} />
      <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1 }}>
        {data.label}
      </span>
      {data.duration != null && data.duration > 0 && (
        <span style={{ fontSize: 10, opacity: 0.6, flexShrink: 0 }}>
          {data.duration >= 1000 ? `${(data.duration / 1000).toFixed(1)}s` : `${data.duration}ms`}
        </span>
      )}
      <Handle
        type="source"
        position={Position.Bottom}
        style={{ visibility: "hidden", pointerEvents: "none" }}
      />
    </div>
  );
}

const runNodeTypes = { runNode: RunNodeComponent };

// useSSELogs subscribes to the SSE log stream for a task while it is running.
// Returns the accumulated live log string (empty string when not streaming).
function useSSELogs(logs: InspectionLogs, runID: string, active: boolean): string {
  const [lines, setLines] = useState<string[]>([]);
  const esRef = useRef<EventSource | null>(null);
  const retryRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const taskAttemptID = logs.task_attempt_id;
  const exact = logs.exact;

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
      const url = inspectionLogStreamURL(
        runID,
        { task_attempt_id: taskAttemptID, exact },
        storedKey
      );
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
  }, [taskAttemptID, exact, runID, active]);

  return lines.join("\n");
}

function TaskRow({
  task,
  runID,
  expanded,
  onToggleExpand,
  onRetry,
  onKill,
  taskRef,
  borderless,
}: {
  task: InspectionCurrentTask;
  runID: string;
  expanded: boolean;
  onToggleExpand: () => void;
  onRetry: (taskID: string) => void;
  onKill: (taskID: string) => void;
  taskRef?: React.Ref<HTMLDivElement>;
  borderless?: boolean;
}) {
  const current = task.current_attempt;
  const metrics = current.metrics;
  const durationMs = inspectionAttemptDuration(current);
  // Only open an SSE stream for tasks that are actively running — queued tasks
  // produce no output yet and each open stream costs a server connection.
  const isLive = current.status === "running";
  const attempts = task.attempts;
  const liveOutput = useSSELogs(current.logs, runID, isLive && expanded);
  const displayOutput = isLive ? liveOutput : current.output;
  const hasOutput = displayOutput && displayOutput.trim().length > 0;
  const hasMultipleAttempts = attempts.length > 1;
  const logRef = useRef<HTMLElement | null>(null);

  // Auto-scroll the log pane to the bottom whenever new output arrives.
  useEffect(() => {
    if (logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight;
    }
  }, [displayOutput]);

  const isExpandable = isLive || current.status === "queued" || hasOutput || hasMultipleAttempts;

  const handleExpand = () => {
    if (isExpandable) {
      onToggleExpand();
    }
  };

  return (
    <Card
      ref={taskRef}
      padding="0"
      mb={borderless ? 0 : "xs"}
      shadow={borderless ? "0" : undefined}
      style={{
        border: borderless
          ? "none"
          : current.status === "failed"
            ? "1px solid var(--mantine-color-red-3)"
            : current.status === "up_for_retry"
              ? "1px solid var(--mantine-color-orange-3)"
              : "1px solid var(--mantine-color-default-border)",
      }}
    >
      <Group
        px="md"
        py="sm"
        justify="space-between"
        style={{ cursor: isExpandable ? "pointer" : "default" }}
        onClick={handleExpand}
      >
        <Group gap="sm">
          <StatusIcon status={current.status} />
          <div>
            <Group gap="xs">
              <Text fw={600} size="sm">
                {task.id}
              </Text>
              {hasMultipleAttempts && (
                <Badge size="xs" variant="dot" color="orange">
                  {inspectionAttemptLabel(current)}
                </Badge>
              )}
              {durationMs !== null && durationMs > 0 && (
                <Badge size="xs" variant="outline" color="gray">
                  {formatDurationMs(durationMs)}
                </Badge>
              )}
              {metrics && metrics.peak_memory_bytes > 0 && (
                <Badge size="xs" variant="outline" color="blue">
                  {metrics.peak_memory_bytes >= 1024 * 1024 * 1024
                    ? `${(metrics.peak_memory_bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`
                    : metrics.peak_memory_bytes >= 1024 * 1024
                      ? `${(metrics.peak_memory_bytes / (1024 * 1024)).toFixed(1)} MB`
                      : `${(metrics.peak_memory_bytes / 1024).toFixed(0)} KB`}
                </Badge>
              )}
              {metrics && metrics.exit_code !== undefined && current.status !== "running" && (
                <Badge
                  size="xs"
                  variant="outline"
                  color={metrics.exit_code === 0 ? "green" : "red"}
                >
                  exit {metrics.exit_code}
                </Badge>
              )}
              {metrics && (metrics.cpu_user_ms > 0 || metrics.cpu_system_ms > 0) && (
                <Badge size="xs" variant="outline" color="violet">
                  CPU {((metrics.cpu_user_ms + metrics.cpu_system_ms) / 1000).toFixed(1)}s
                </Badge>
              )}
            </Group>
            <Text size="xs" c="dimmed">
              Last Updated {new Date(current.updated_at).toLocaleTimeString()}
            </Text>
            {current.item_value !== null && (
              <Text size="xs" c="dimmed" lineClamp={1}>
                Item: {current.item_value}
              </Text>
            )}
          </div>
        </Group>
        <Group gap="sm">
          <Badge color={getStatusColor(current.status)} variant="light" radius="xl" size="sm">
            {getStatusLabel(current.status)}
          </Badge>
          {(current.status === "success" ||
            current.status === "failed" ||
            current.status === "cancelled") && (
            <Tooltip label="Retry Task">
              <ActionIcon
                variant="light"
                color="blue"
                size="sm"
                onClick={(e) => {
                  e.stopPropagation();
                  onRetry(task.id);
                }}
              >
                <IconPlayerPlay size={12} />
              </ActionIcon>
            </Tooltip>
          )}
          {current.status === "running" && (
            <Tooltip label="Kill Task">
              <ActionIcon
                variant="light"
                color="red"
                size="sm"
                onClick={(e) => {
                  e.stopPropagation();
                  onKill(task.id);
                }}
              >
                <IconPlayerStop size={12} />
              </ActionIcon>
            </Tooltip>
          )}
          {isExpandable && (
            <ActionIcon variant="transparent" size="sm" color="dimmed">
              {expanded ? <IconChevronDown size={16} /> : <IconChevronRight size={16} />}
            </ActionIcon>
          )}
        </Group>
      </Group>

      <Collapse in={expanded}>
        <Divider />
        {hasMultipleAttempts ? (
          <Tabs defaultValue={current.id} style={{ backgroundColor: "var(--log-bg)" }}>
            <Tabs.List px="md" pt="xs">
              {attempts.map((a) => (
                <Tabs.Tab key={a.id} value={a.id} leftSection={<StatusIcon status={a.status} />}>
                  <Text size="xs" fw={600}>
                    {inspectionAttemptLabel(a)}
                  </Text>
                </Tabs.Tab>
              ))}
            </Tabs.List>
            {attempts.map((a) => (
              <Tabs.Panel key={a.id} value={a.id} p="md">
                <Stack gap={2} mb="xs">
                  <Text size="xs" c="dimmed">
                    Generation {a.generation} · generation attempt #{a.attempt}
                  </Text>
                  <Text size="xs" c="dimmed">
                    Updated {new Date(a.updated_at).toLocaleString()}
                    {a.started_at ? ` · Started ${new Date(a.started_at).toLocaleString()}` : ""}
                    {a.completed_at
                      ? ` · Completed ${new Date(a.completed_at).toLocaleString()}`
                      : ""}
                    {inspectionAttemptDuration(a) !== null
                      ? ` · ${formatDurationMs(inspectionAttemptDuration(a)!)}`
                      : ""}
                  </Text>
                  {a.item_value !== null && (
                    <Text size="xs" c="dimmed">
                      Item: {a.item_value}
                    </Text>
                  )}
                </Stack>

                {a.command && <LogTerminal label="Command" content={a.command} />}

                <LogTerminal
                  label="Output Log"
                  content={a.output || "No output for this attempt."}
                  isFailed={a.status === "failed"}
                />
              </Tabs.Panel>
            ))}
          </Tabs>
        ) : (
          <Box p="md" style={{ backgroundColor: "var(--log-bg)" }}>
            {current.command && <LogTerminal label="Command" content={current.command} />}

            <LogTerminal
              ref={logRef}
              label="Output Log"
              isLive={isLive}
              content={
                displayOutput || (isLive ? "Waiting for output..." : "No output generated yet.")
              }
              isFailed={current.status === "failed"}
            />
          </Box>
        )}
      </Collapse>
    </Card>
  );
}

function RunDetailsContent() {
  const searchParams = useSearchParams();
  const id = searchParams.get("id");
  const taskParam = searchParams.get("task");
  const router = useRouter();
  const [inspection, setInspection] = useState<RunInspection | null>(null);
  const [loading, setLoading] = useState(true);
  const tasks = useMemo(() => (inspection ? inspectionCurrentTasks(inspection) : []), [inspection]);
  const run = inspection?.run ?? null;
  // Lifted expanded state keyed by task instance ID — prevents poll-driven
  // re-renders from resetting the open/closed state of each TaskRow.
  const [expandedMap, setExpandedMap] = useState<Record<string, boolean>>({});
  // Refs for scrolling to a specific task
  const taskRefs = useRef<Record<string, HTMLDivElement | null>>({});
  const didScrollToTask = useRef(false);

  // React Flow graph state
  const [graphNodes, setGraphNodes, onNodesChange] = useNodesState<Node>([]);
  const [graphEdges, setGraphEdges, onEdgesChange] = useEdgesState<Edge>([]);
  // Track previous node IDs so we only re-layout when structure changes
  const prevNodeIdsRef = useRef<string>("");
  const activeRunIDRef = useRef<string | null>(id);

  const fetchTasks = useCallback(async () => {
    if (!id) return;
    const requestedID = id;
    try {
      const nextInspection = await fetchRunInspection(id, apiFetch);
      if (activeRunIDRef.current !== requestedID) return;
      const nextTasks = inspectionCurrentTasks(nextInspection);
      setInspection(nextInspection);
      // Auto-expand running/failed/retry tasks on first load (only when not
      // already tracked in the map so we don't clobber user-toggled state).
      // If a &task= param is present, force-expand that task instead.
      setExpandedMap((prev) => {
        const next = { ...prev };
        for (const task of nextTasks) {
          const current = task.current_attempt;
          if (!(current.id in next)) {
            if (taskParam && task.id === taskParam) {
              next[current.id] = true;
            } else if (!taskParam) {
              next[current.id] =
                current.status === "running" ||
                current.status === "queued" ||
                current.status === "failed" ||
                current.status === "up_for_retry";
            } else {
              next[current.id] = false;
            }
          }
        }
        return next;
      });
    } catch (err) {
      console.error("Failed to fetch tasks", err);
    } finally {
      if (activeRunIDRef.current === requestedID) setLoading(false);
    }
  }, [id, taskParam]);

  useEffect(() => {
    activeRunIDRef.current = id;
    setInspection(null);
    setExpandedMap({});
    setCollapsedGroups({});
    setGraphNodes([]);
    setGraphEdges([]);
    prevNodeIdsRef.current = "";
    didScrollToTask.current = false;
    setLoading(true);
  }, [id, setGraphNodes, setGraphEdges]);

  // Poll for task/run updates. useVisibilityPoll pauses when the tab is hidden.
  // Skip the poll once the run reaches a terminal state.
  useVisibilityPoll(
    () => {
      if (run && TERMINAL_STATUSES.has(run.status)) return;
      fetchTasks();
    },
    5000,
    [fetchTasks, run]
  );

  // Scroll to the targeted task on first load when &task= is present
  useEffect(() => {
    if (!taskParam || didScrollToTask.current || tasks.length === 0) return;
    const target = tasks.find((task) => task.id === taskParam);
    if (target && taskRefs.current[target.current_attempt.id]) {
      // Small delay to let the Collapse animation open
      setTimeout(() => {
        taskRefs.current[target.current_attempt.id]?.scrollIntoView({
          behavior: "smooth",
          block: "start",
        });
      }, 150);
      didScrollToTask.current = true;
    }
  }, [taskParam, tasks]);

  // Build/update graph whenever the inspection read model changes.
  // Only re-layout (dagre) when the set of node IDs changes — on pure status
  // updates we just patch node data to avoid the graph jumping around.
  const topology = useMemo(
    () =>
      inspection ? projectInspectionTopology(inspection) : { nodes: [], edges: [], stages: [] },
    [inspection]
  );

  useEffect(() => {
    const nodes: Node[] = topology.nodes.map((node) => ({
      id: node.id,
      type: "runNode",
      data: {
        label: node.id,
        status: node.status ?? "pending",
        duration: node.runtimeTask
          ? inspectionAttemptDuration(node.runtimeTask.logicalTask.current_attempt)
          : null,
      },
      position: { x: 0, y: 0 },
    }));
    const statusByID = new Map(topology.nodes.map((node) => [node.id, node.status]));
    const edges: Edge[] = topology.edges.map((edge) => {
      const sourceStatus = statusByID.get(edge.source);
      const color =
        sourceStatus === "success"
          ? "var(--mantine-color-green-filled)"
          : sourceStatus === "failed"
            ? "var(--mantine-color-red-filled)"
            : sourceStatus === "running"
              ? "var(--mantine-color-blue-filled)"
              : "var(--mantine-color-dimmed)";
      return {
        ...edge,
        animated: sourceStatus === "running",
        style: { stroke: color, strokeWidth: 1.5 },
        markerEnd: { type: MarkerType.ArrowClosed, color, width: 14, height: 14 },
      };
    });

    const nodeIdStr = nodes
      .map((n) => n.id)
      .sort()
      .join(",");
    if (nodeIdStr !== prevNodeIdsRef.current) {
      // Structure changed — full re-layout
      prevNodeIdsRef.current = nodeIdStr;
      setGraphNodes(layoutTopologyStages(nodes, topology.stages, RUN_NODE_W, RUN_NODE_H));
      setGraphEdges(edges);
    } else {
      // Only status/duration changed — patch data in place, keep positions
      setGraphNodes((prev) =>
        prev.map((n) => {
          const updated = nodes.find((u) => u.id === n.id);
          return updated ? { ...n, data: updated.data } : n;
        })
      );
      setGraphEdges(edges);
    }
  }, [topology, setGraphNodes, setGraphEdges]);

  // Stable graph height — based on definition count (doesn't change as statuses update).
  const graphHeight = useMemo(() => {
    const definitions =
      inspection?.logical_tasks.filter((task) => task.kind === "definition").length ?? 0;
    if (definitions === 0) return 250;
    return Math.max(200, Math.min(500, definitions * 80));
  }, [inspection]);

  // When the run first enters a terminal state, do one final fetch after a
  // short delay. This catches task output that is written to the DB in the
  // same tick as the status transition (the normal poll may have landed in
  // the narrow window before the output column was updated).
  const prevRunStatusRef = useRef<string | null>(null);
  useEffect(() => {
    if (!run) return;
    const wasTerminal =
      prevRunStatusRef.current !== null && TERMINAL_STATUSES.has(prevRunStatusRef.current);
    const isTerminal = TERMINAL_STATUSES.has(run.status);
    if (isTerminal && !wasTerminal) {
      const timer = setTimeout(fetchTasks, 500);
      prevRunStatusRef.current = run.status;
      return () => clearTimeout(timer);
    }
    prevRunStatusRef.current = run.status;
  }, [run, fetchTasks]);

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

  // Compute elapsed time — use LiveElapsed component for running runs
  const staticElapsed = run
    ? run.duration_ms !== null
      ? formatElapsed(Math.max(1, Math.floor(run.duration_ms / 1000)))
      : null
    : null;

  // Update the URL &task= param when a task is expanded/collapsed
  const toggleTask = useCallback(
    (task: InspectionCurrentTask) => {
      const attemptID = task.current_attempt.id;
      const willExpand = !expandedMap[attemptID];
      setExpandedMap((prev) => ({ ...prev, [attemptID]: willExpand }));
      const params = new URLSearchParams(window.location.search);
      if (willExpand) {
        params.set("task", task.id);
      } else if (params.get("task") === task.id) {
        params.delete("task");
      }
      router.replace(`/runs?${params.toString()}`, { scroll: false });
    },
    [expandedMap, router]
  );

  // Handle clicking a node in the graph — expand + scroll to that task
  const handleNodeClick = useCallback(
    (_: React.MouseEvent, node: Node) => {
      const task = tasks.find((candidate) => candidate.id === node.id);
      if (!task) return;
      const attemptID = task.current_attempt.id;
      // Expand if not already
      if (!expandedMap[attemptID]) {
        toggleTask(task);
      }
      // Scroll to it
      setTimeout(() => {
        taskRefs.current[attemptID]?.scrollIntoView({ behavior: "smooth", block: "start" });
      }, 150);
    },
    [tasks, expandedMap, toggleTask]
  );

  // Collapsed state for map groups (keyed by parent taskID)
  const [collapsedGroups, setCollapsedGroups] = useState<Record<string, boolean>>({});

  const taskSegments = useMemo(() => segmentInspectionTasks(tasks, topology), [tasks, topology]);
  const taskSummary = useMemo(() => summarizeInspectionTasks(tasks), [tasks]);

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
      <PageHeader
        title={id as string}
        showBack
        backTo="/runs"
        subtitle={run ? `Started at ${new Date(run.created_at).toLocaleString()}` : undefined}
        badge={
          run ? (
            <Badge size="sm" color={getStatusColor(run.status)} variant="light" radius="xl">
              {getStatusLabel(run.status)}
            </Badge>
          ) : undefined
        }
        actions={
          <Group gap="sm">
            {run && run.status === "running" && (
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
        }
      />

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
                  onClick={() => router.push(`/dags?id=${run.dag_id}`)}
                >
                  {run.dag_id}
                </Text>
              </div>
              <div>
                <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                  Trigger
                </Text>
                <Text fw={600} size="sm" mt={4}>
                  {run.trigger_type === "manual"
                    ? "Manual"
                    : run.trigger_type === "scheduled"
                      ? "Scheduled"
                      : "Triggered"}
                </Text>
              </div>
              <div>
                <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                  Started At
                </Text>
                <Text fw={600} size="sm" mt={4}>
                  {new Date(run.created_at).toLocaleString()}
                </Text>
              </div>
              <div>
                <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                  Duration
                </Text>
                <Box mt={4}>
                  {run.status === "running" ? (
                    <LiveElapsed startedAt={run.created_at} />
                  ) : (
                    <Text fw={600} size="sm">
                      {staticElapsed ?? "—"}
                    </Text>
                  )}
                </Box>
              </div>
              <div>
                <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                  Tasks
                </Text>
                <Group gap={4} mt={4}>
                  {taskSummary.success > 0 && (
                    <Badge size="xs" color="green" variant="light">
                      {taskSummary.success} ok
                    </Badge>
                  )}
                  {taskSummary.failed > 0 && (
                    <Badge size="xs" color="red" variant="light">
                      {taskSummary.failed} failed
                    </Badge>
                  )}
                  {taskSummary.running > 0 && (
                    <Badge size="xs" color="blue" variant="light">
                      {taskSummary.running} running
                    </Badge>
                  )}
                  {taskSummary.retry > 0 && (
                    <Badge size="xs" color="orange" variant="light">
                      {taskSummary.retry} retry
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
            {/* Progress bar for running runs */}
            {run.status === "running" && taskSummary.total > 0 && (
              <Box mt="sm">
                <Group justify="space-between" mb={4}>
                  <Text size="xs" c="dimmed">
                    {taskSummary.completed} of {taskSummary.total} tasks complete
                  </Text>
                  <Text size="xs" c="dimmed">
                    {Math.round(taskSummary.progress_percent)}%
                  </Text>
                </Group>
                <Progress value={taskSummary.progress_percent} color="blue" size="sm" radius="xl" />
              </Box>
            )}
          </Card>
        )
      )}

      {/* DAG dependency graph with live status */}
      {graphNodes.length > 0 && (
        <Card
          padding="0"
          mb="xl"
          shadow="sm"
          radius="md"
          withBorder
          style={{ overflow: "hidden", position: "relative" }}
        >
          <div style={{ height: `${graphHeight}px` }}>
            <ReactFlow
              nodes={graphNodes}
              edges={graphEdges}
              onNodesChange={onNodesChange}
              onEdgesChange={onEdgesChange}
              nodeTypes={runNodeTypes}
              onNodeClick={handleNodeClick}
              fitView
              fitViewOptions={{ padding: 0.2 }}
              nodesDraggable={false}
              nodesConnectable={false}
              elementsSelectable={false}
              panOnDrag
              zoomOnScroll={false}
              minZoom={0.5}
              maxZoom={1.5}
              proOptions={{ hideAttribution: true }}
            >
              <Background color="var(--graph-grid)" gap={16} />
            </ReactFlow>
          </div>
          <Text
            size="10px"
            c="dimmed"
            style={{ position: "absolute", bottom: 4, right: 8, opacity: 0.6 }}
          >
            Click a node to view logs
          </Text>
        </Card>
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
        <Stack gap={0}>
          {taskSegments.map((seg, idx) => {
            // Stage divider: insert when depth changes from previous segment
            const prevDepth = idx > 0 ? taskSegments[idx - 1].depth : seg.depth;
            const showDivider = idx > 0 && seg.depth !== prevDepth;

            if (seg.kind === "single") {
              const attemptID = seg.task.current_attempt.id;
              return (
                <Box key={attemptID}>
                  {showDivider && (
                    <Divider
                      my="sm"
                      color="var(--mantine-color-default-border)"
                      style={{ opacity: 0.5 }}
                    />
                  )}
                  <Box mb="xs">
                    <TaskRow
                      task={seg.task}
                      runID={id}
                      expanded={!!expandedMap[attemptID]}
                      onToggleExpand={() => toggleTask(seg.task)}
                      onRetry={handleRetry}
                      onKill={handleKillTask}
                      taskRef={(el) => {
                        taskRefs.current[attemptID] = el;
                      }}
                    />
                  </Box>
                </Box>
              );
            }

            // Map group: parent with collapsible children
            const { parent, children } = seg;
            const parentAttemptID = parent.current_attempt.id;
            const isGroupCollapsed = collapsedGroups[parent.id] ?? true;
            const childSuccess = children.filter(
              (child) => child.current_attempt.status === "success"
            ).length;
            const childFailed = children.filter(
              (child) => child.current_attempt.status === "failed"
            ).length;
            const childRunning = children.filter(
              (child) => child.current_attempt.status === "running"
            ).length;

            return (
              <Box key={parentAttemptID}>
                {showDivider && (
                  <Divider
                    my="sm"
                    color="var(--mantine-color-default-border)"
                    style={{ opacity: 0.5 }}
                  />
                )}
                {/* Map group: parent + children toggle + collapsible children
                    wrapped in a single bordered container */}
                <Card
                  padding="0"
                  mb="xs"
                  style={{
                    border: "1px solid var(--mantine-color-default-border)",
                    overflow: "hidden",
                  }}
                >
                  {/* Parent task row (rendered without its own Card border) */}
                  <TaskRow
                    task={parent}
                    runID={id}
                    expanded={!!expandedMap[parentAttemptID]}
                    onToggleExpand={() => toggleTask(parent)}
                    onRetry={handleRetry}
                    onKill={handleKillTask}
                    taskRef={(el) => {
                      taskRefs.current[parentAttemptID] = el;
                    }}
                    borderless
                  />
                  {/* Children toggle footer */}
                  <Group
                    gap="xs"
                    px="md"
                    py={6}
                    style={{
                      cursor: "pointer",
                      borderTop: "1px solid var(--mantine-color-default-border)",
                      background: "var(--mantine-color-dark-7, var(--mantine-color-gray-0))",
                      userSelect: "none",
                    }}
                    onClick={() =>
                      setCollapsedGroups((prev) => ({
                        ...prev,
                        [parent.id]: !isGroupCollapsed,
                      }))
                    }
                  >
                    <ActionIcon variant="transparent" size="xs" color="dimmed">
                      {isGroupCollapsed ? (
                        <IconChevronRight size={14} />
                      ) : (
                        <IconChevronDown size={14} />
                      )}
                    </ActionIcon>
                    <Text size="xs" c="dimmed" fw={500}>
                      {children.length} map {children.length === 1 ? "child" : "children"}
                    </Text>
                    {childSuccess > 0 && (
                      <Badge size="xs" variant="dot" color="green">
                        {childSuccess}
                      </Badge>
                    )}
                    {childFailed > 0 && (
                      <Badge size="xs" variant="dot" color="red">
                        {childFailed}
                      </Badge>
                    )}
                    {childRunning > 0 && (
                      <Badge size="xs" variant="dot" color="blue">
                        {childRunning}
                      </Badge>
                    )}
                  </Group>
                  {/* Collapsible children list */}
                  <Collapse in={!isGroupCollapsed}>
                    <Box
                      px="sm"
                      pb="sm"
                      pt={4}
                      style={{
                        borderTop: "1px solid var(--mantine-color-default-border)",
                      }}
                    >
                      <Stack gap={4}>
                        {children.map((child) => {
                          const childAttemptID = child.current_attempt.id;
                          return (
                            <TaskRow
                              key={childAttemptID}
                              task={child}
                              runID={id}
                              expanded={!!expandedMap[childAttemptID]}
                              onToggleExpand={() => toggleTask(child)}
                              onRetry={handleRetry}
                              onKill={handleKillTask}
                              taskRef={(el) => {
                                taskRefs.current[childAttemptID] = el;
                              }}
                            />
                          );
                        })}
                      </Stack>
                    </Box>
                  </Collapse>
                </Card>
              </Box>
            );
          })}
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

  useVisibilityPoll(fetchData, 5000, [fetchData]);

  return (
    <>
      <PageHeader
        title="Runs"
        actions={
          <Button leftSection={<IconRefresh size={16} />} variant="light" onClick={fetchData}>
            Refresh
          </Button>
        }
      />

      <RunsTable
        runs={runs}
        loading={loading}
        dags={dags}
        totalRuns={totalRuns}
        limit={limit}
        page={page}
        onPageChange={setPage}
        dagFilter={dagFilter}
        onDagFilterChange={setDagFilter}
        statusFilter={statusFilter}
        onStatusFilterChange={setStatusFilter}
        triggerFilter={triggerFilter}
        onTriggerFilterChange={setTriggerFilter}
        onRunKilled={fetchData}
      />
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
