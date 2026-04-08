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
import dagre from "dagre";
import { PageHeader } from "../../components/ui/PageHeader";
import { StatusIcon } from "../../components/ui/StatusIcon";
import { getStatusColor, getStatusLabel } from "../../components/ui/StatusBadge";
import { LogTerminal } from "../../components/blocks/LogTerminal";
import { RunsTable, Run } from "../../components/blocks/RunsTable";

// DAG definition task — used to derive dependency edges
interface DagTaskDef {
  ID: string;
  Type: string;
  Command: string;
  DependsOn: string[] | null;
  MapOver?: string;
}

interface DagDef {
  ID: string;
  Tasks: DagTaskDef[];
}

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

/** Strip [N] suffix to get the base task ID */
function baseTaskID(taskID: string): string {
  const idx = taskID.indexOf("[");
  return idx !== -1 ? taskID.substring(0, idx) : taskID;
}

/** Build React Flow nodes + edges from DAG definition + runtime task instances */
function buildRunGraph(
  dagTasks: DagTaskDef[],
  runTasks: RunTask[]
): { nodes: Node[]; edges: Edge[] } {
  const dagMap = new Map<string, DagTaskDef>();
  dagTasks.forEach((t) => dagMap.set(t.ID, t));

  // Identify map parents that have ACTUAL children in the runtime list.
  // A parent without children yet (still pending/running before fan-out)
  // should be shown as a regular node.
  const childrenOf = new Map<string, RunTask[]>();

  runTasks.forEach((t) => {
    const base = baseTaskID(t.TaskID);
    if (base !== t.TaskID) {
      if (!childrenOf.has(base)) childrenOf.set(base, []);
      childrenOf.get(base)!.push(t);
    }
  });

  // Only consider a parent "expanded" (hidden in favour of children) if
  // children actually exist in the task list.
  const expandedParents = new Set<string>(childrenOf.keys());

  // Build a lookup: taskID -> RunTask (latest attempt only)
  const instanceOf = new Map<string, RunTask>();
  runTasks.forEach((t) => instanceOf.set(t.TaskID, t));

  // --- Nodes ---
  const nodes: Node[] = [];
  runTasks.forEach((t) => {
    if (expandedParents.has(t.TaskID)) return; // hide parent — children shown instead
    nodes.push({
      id: t.TaskID,
      type: "runNode",
      data: {
        label: t.TaskID,
        status: t.Status,
        duration: t.Metrics?.DurationMs,
      },
      position: { x: 0, y: 0 },
    });
  });

  // --- Edges ---
  const edges: Edge[] = [];
  const edgeSet = new Set<string>(); // dedup
  const addEdge = (src: string, tgt: string) => {
    const key = `${src}->${tgt}`;
    if (edgeSet.has(key)) return;
    edgeSet.add(key);
    const srcTask = instanceOf.get(src);
    const color =
      srcTask?.Status === "success"
        ? "var(--mantine-color-green-filled)"
        : srcTask?.Status === "failed"
          ? "var(--mantine-color-red-filled)"
          : srcTask?.Status === "running"
            ? "var(--mantine-color-blue-filled)"
            : "var(--mantine-color-dimmed)";
    edges.push({
      id: `e-${src}-${tgt}`,
      source: src,
      target: tgt,
      animated: srcTask?.Status === "running",
      style: { stroke: color, strokeWidth: 1.5 },
      markerEnd: { type: MarkerType.ArrowClosed, color, width: 14, height: 14 },
    });
  };

  runTasks.forEach((t) => {
    if (expandedParents.has(t.TaskID)) return;

    const base = baseTaskID(t.TaskID);
    const isChild = base !== t.TaskID;
    const dagTask = dagMap.get(base);
    if (!dagTask) return;

    if (isChild) {
      // Map child — connect from each dependency of the parent definition
      dagTask.DependsOn?.forEach((dep) => {
        if (!expandedParents.has(dep) && instanceOf.has(dep)) {
          addEdge(dep, t.TaskID);
        }
      });
    } else {
      // Regular task (or map parent whose children haven't appeared yet)
      dagTask.DependsOn?.forEach((dep) => {
        if (expandedParents.has(dep)) {
          // Dependency is a map parent with children — fan-in from each child
          childrenOf.get(dep)?.forEach((child) => addEdge(child.TaskID, t.TaskID));
        } else if (instanceOf.has(dep)) {
          addEdge(dep, t.TaskID);
        }
      });
    }
  });

  return { nodes, edges };
}

/** Dagre auto-layout for run graph */
function layoutRunGraph(nodes: Node[], edges: Edge[]): { nodes: Node[]; edges: Edge[] } {
  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({ rankdir: "TB", ranksep: 40, nodesep: 20 });

  nodes.forEach((n) => g.setNode(n.id, { width: RUN_NODE_W, height: RUN_NODE_H }));
  edges.forEach((e) => g.setEdge(e.source, e.target));
  dagre.layout(g);

  const layouted = nodes.map((n) => {
    const pos = g.node(n.id);
    return {
      ...n,
      position: { x: pos.x - RUN_NODE_W / 2, y: pos.y - RUN_NODE_H / 2 },
      targetPosition: Position.Top,
      sourcePosition: Position.Bottom,
    };
  });

  return { nodes: layouted, edges };
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
  expanded,
  onToggleExpand,
  onRetry,
  onKill,
  taskRef,
  borderless,
}: {
  task: RunTask;
  runID: string;
  expanded: boolean;
  onToggleExpand: () => void;
  onRetry: (taskID: string) => void;
  onKill: (taskID: string) => void;
  taskRef?: React.Ref<HTMLDivElement>;
  borderless?: boolean;
}) {
  // Only open an SSE stream for tasks that are actively running — queued tasks
  // produce no output yet and each open stream costs a server connection.
  const isLive = task.Status === "running";
  const [attempts, setAttempts] = useState<RunTask[]>([]);
  const [loadingAttempts, setLoadingAttempts] = useState(false);
  const liveOutput = useSSELogs(task.ID, runID, isLive && expanded);
  const displayOutput = isLive ? liveOutput : task.Output;
  const hasOutput = displayOutput && displayOutput.trim().length > 0;
  const hasMultipleAttempts = task.Attempt > 1;
  const logRef = useRef<HTMLElement | null>(null);

  // Auto-scroll the log pane to the bottom whenever new output arrives.
  useEffect(() => {
    if (logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight;
    }
  }, [displayOutput]);

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

  const isExpandable = isLive || task.Status === "queued" || hasOutput || hasMultipleAttempts;

  const handleExpand = () => {
    if (isExpandable) {
      if (!expanded) fetchAttempts();
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
          : task.Status === "failed"
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
        style={{ cursor: isExpandable ? "pointer" : "default" }}
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
                <Badge size="xs" variant="outline" color="blue">
                  {task.Metrics.PeakMemoryBytes >= 1024 * 1024 * 1024
                    ? `${(task.Metrics.PeakMemoryBytes / (1024 * 1024 * 1024)).toFixed(1)} GB`
                    : task.Metrics.PeakMemoryBytes >= 1024 * 1024
                      ? `${(task.Metrics.PeakMemoryBytes / (1024 * 1024)).toFixed(1)} MB`
                      : `${(task.Metrics.PeakMemoryBytes / 1024).toFixed(0)} KB`}
                </Badge>
              )}
              {task.Metrics && task.Metrics.ExitCode !== undefined && task.Status !== "running" && (
                <Badge
                  size="xs"
                  variant="outline"
                  color={task.Metrics.ExitCode === 0 ? "green" : "red"}
                >
                  exit {task.Metrics.ExitCode}
                </Badge>
              )}
              {task.Metrics && (task.Metrics.CpuUserMs > 0 || task.Metrics.CpuSystemMs > 0) && (
                <Badge size="xs" variant="outline" color="violet">
                  CPU {((task.Metrics.CpuUserMs + task.Metrics.CpuSystemMs) / 1000).toFixed(1)}s
                </Badge>
              )}
            </Group>
            <Text size="xs" c="dimmed">
              Last Updated {new Date(task.UpdatedAt).toLocaleTimeString()}
            </Text>
          </div>
        </Group>
        <Group gap="sm">
          <Badge color={getStatusColor(task.Status)} variant="light" radius="xl" size="sm">
            {getStatusLabel(task.Status)}
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
          {isExpandable && (
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

                {a.Command && <LogTerminal label="Command" content={a.Command} />}

                <LogTerminal
                  label="Output Log"
                  content={a.Output || "No output for this attempt."}
                  isFailed={a.Status === "failed"}
                />
              </Tabs.Panel>
            ))}
          </Tabs>
        ) : (
          <Box p="md" style={{ backgroundColor: "var(--log-bg)" }}>
            {task.Command && <LogTerminal label="Command" content={task.Command} />}

            <LogTerminal
              ref={logRef}
              label="Output Log"
              isLive={isLive}
              content={
                displayOutput || (isLive ? "Waiting for output..." : "No output generated yet.")
              }
              isFailed={task.Status === "failed"}
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
  const [tasks, setTasks] = useState<RunTask[]>([]);
  const [run, setRun] = useState<Run | null>(null);
  const [dagDef, setDagDef] = useState<DagTaskDef[]>([]);
  const [loading, setLoading] = useState(true);
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
  // Use a ref for dagDef loading guard so it doesn't destabilize fetchTasks
  const dagDefRef = useRef<DagTaskDef[]>([]);

  const TERMINAL = new Set(["success", "failed", "cancelled"]);

  const fetchTasks = useCallback(async () => {
    if (!id) return;
    try {
      const needDagDef = dagDefRef.current.length === 0;
      const [tasksRes, runRes, dagsRes] = await Promise.all([
        apiFetch(`/api/runs/${id}/tasks`),
        apiFetch(`/api/runs/${id}`),
        needDagDef ? apiFetch("/api/dags") : Promise.resolve(null),
      ]);
      if (tasksRes.ok) {
        const newTasks: RunTask[] = await tasksRes.json();
        setTasks(newTasks);
        // Auto-expand running/failed/retry tasks on first load (only when not
        // already tracked in the map so we don't clobber user-toggled state).
        // If a &task= param is present, force-expand that task instead.
        setExpandedMap((prev) => {
          const next = { ...prev };
          for (const t of newTasks) {
            if (!(t.ID in next)) {
              if (taskParam && t.TaskID === taskParam) {
                next[t.ID] = true;
              } else if (!taskParam) {
                next[t.ID] =
                  t.Status === "running" ||
                  t.Status === "queued" ||
                  t.Status === "failed" ||
                  t.Status === "up_for_retry";
              } else {
                next[t.ID] = false;
              }
            }
          }
          return next;
        });
      }
      let fetchedRun: Run | null = null;
      if (runRes.ok) {
        fetchedRun = await runRes.json();
        setRun(fetchedRun);
      }
      // Fetch DAG definition once to get task dependency info
      if (dagsRes && dagsRes.ok && fetchedRun) {
        const allDags: DagDef[] = await dagsRes.json();
        const dag = allDags.find((d) => d.ID === fetchedRun!.DAGID);
        if (dag?.Tasks) {
          dagDefRef.current = dag.Tasks;
          setDagDef(dag.Tasks);
        }
      }
    } catch (err) {
      console.error("Failed to fetch tasks", err);
    } finally {
      setLoading(false);
    }
  }, [id, taskParam]);

  // Poll for task/run updates. useVisibilityPoll pauses when the tab is hidden.
  // Skip the poll once the run reaches a terminal state.
  useVisibilityPoll(
    () => {
      if (run && TERMINAL.has(run.Status)) return;
      fetchTasks();
    },
    5000,
    [fetchTasks, run]
  );

  // Scroll to the targeted task on first load when &task= is present
  useEffect(() => {
    if (!taskParam || didScrollToTask.current || tasks.length === 0) return;
    const target = tasks.find((t) => t.TaskID === taskParam);
    if (target && taskRefs.current[target.ID]) {
      // Small delay to let the Collapse animation open
      setTimeout(() => {
        taskRefs.current[target.ID]?.scrollIntoView({ behavior: "smooth", block: "start" });
      }, 150);
      didScrollToTask.current = true;
    }
  }, [taskParam, tasks]);

  // Build/update graph whenever tasks or DAG definition change.
  // Only re-layout (dagre) when the set of node IDs changes — on pure status
  // updates we just patch node data to avoid the graph jumping around.
  useEffect(() => {
    if (dagDef.length === 0 || tasks.length === 0) return;
    const { nodes, edges } = buildRunGraph(dagDef, tasks);

    const nodeIdStr = nodes
      .map((n) => n.id)
      .sort()
      .join(",");
    if (nodeIdStr !== prevNodeIdsRef.current) {
      // Structure changed — full re-layout
      prevNodeIdsRef.current = nodeIdStr;
      const laid = layoutRunGraph(nodes, edges);
      setGraphNodes(laid.nodes);
      setGraphEdges(laid.edges);
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
  }, [dagDef, tasks, setGraphNodes, setGraphEdges]);

  // Stable graph height — based on DAG definition task count (doesn't change)
  const graphHeight = useMemo(() => {
    if (dagDef.length === 0) return 250;
    return Math.max(200, Math.min(500, dagDef.length * 80));
  }, [dagDef]);

  // When the run first enters a terminal state, do one final fetch after a
  // short delay. This catches task output that is written to the DB in the
  // same tick as the status transition (the normal poll may have landed in
  // the narrow window before the output column was updated).
  const prevRunStatusRef = useRef<string | null>(null);
  useEffect(() => {
    if (!run) return;
    const wasTerminal = prevRunStatusRef.current !== null && TERMINAL.has(prevRunStatusRef.current);
    const isTerminal = TERMINAL.has(run.Status);
    if (isTerminal && !wasTerminal) {
      const timer = setTimeout(fetchTasks, 500);
      prevRunStatusRef.current = run.Status;
      return () => clearTimeout(timer);
    }
    prevRunStatusRef.current = run.Status;
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
    ? run.CompletedAt
      ? formatElapsed(
          Math.max(
            1,
            Math.floor(
              (new Date(run.CompletedAt).getTime() - new Date(run.CreatedAt).getTime()) / 1000
            )
          )
        )
      : null
    : null;

  // Update the URL &task= param when a task is expanded/collapsed
  const toggleTask = (task: RunTask) => {
    const willExpand = !expandedMap[task.ID];
    setExpandedMap((prev) => ({ ...prev, [task.ID]: willExpand }));
    const params = new URLSearchParams(window.location.search);
    if (willExpand) {
      params.set("task", task.TaskID);
    } else if (params.get("task") === task.TaskID) {
      params.delete("task");
    }
    router.replace(`/runs?${params.toString()}`, { scroll: false });
  };

  // Handle clicking a node in the graph — expand + scroll to that task
  const handleNodeClick = useCallback(
    (_: React.MouseEvent, node: Node) => {
      const task = tasks.find((t) => t.TaskID === node.id);
      if (!task) return;
      // Expand if not already
      if (!expandedMap[task.ID]) {
        toggleTask(task);
      }
      // Scroll to it
      setTimeout(() => {
        taskRefs.current[task.ID]?.scrollIntoView({ behavior: "smooth", block: "start" });
      }, 150);
    },
    [tasks, expandedMap, toggleTask]
  );

  // Collapsed state for map groups (keyed by parent taskID)
  const [collapsedGroups, setCollapsedGroups] = useState<Record<string, boolean>>({});

  // Build structured task segments: groups tasks by DAG definition order,
  // nests map children under their parent, and computes dependency depth
  // for stage dividers.
  type TaskSegment =
    | { kind: "single"; task: RunTask; depth: number }
    | { kind: "map-group"; parent: RunTask; children: RunTask[]; depth: number };

  const taskSegments = useMemo((): TaskSegment[] => {
    if (dagDef.length === 0) {
      return tasks.map((t) => ({ kind: "single" as const, task: t, depth: 0 }));
    }
    // Build a position map from the DAG definition order.
    const defOrder = new Map<string, number>();
    dagDef.forEach((d, i) => defOrder.set(d.ID, i));

    // Compute dependency depth per definition task (longest path from root).
    const dagMap = new Map<string, DagTaskDef>();
    dagDef.forEach((d) => dagMap.set(d.ID, d));
    const depthCache = new Map<string, number>();
    function getDepth(taskId: string): number {
      if (depthCache.has(taskId)) return depthCache.get(taskId)!;
      const def = dagMap.get(taskId);
      if (!def || !def.DependsOn || def.DependsOn.length === 0) {
        depthCache.set(taskId, 0);
        return 0;
      }
      const d = 1 + Math.max(...def.DependsOn.map(getDepth));
      depthCache.set(taskId, d);
      return d;
    }
    dagDef.forEach((d) => getDepth(d.ID));

    // Sort tasks by definition order, parent before children, children by index
    const sorted = [...tasks].sort((a, b) => {
      const baseA = baseTaskID(a.TaskID);
      const baseB = baseTaskID(b.TaskID);
      const posA = defOrder.get(baseA) ?? 999;
      const posB = defOrder.get(baseB) ?? 999;
      if (posA !== posB) return posA - posB;
      const isChildA = baseA !== a.TaskID;
      const isChildB = baseB !== b.TaskID;
      if (!isChildA && isChildB) return -1;
      if (isChildA && !isChildB) return 1;
      const idxA = parseInt(a.TaskID.match(/\[(\d+)\]/)?.[1] ?? "0", 10);
      const idxB = parseInt(b.TaskID.match(/\[(\d+)\]/)?.[1] ?? "0", 10);
      return idxA - idxB;
    });

    // Group into segments
    const segments: TaskSegment[] = [];
    let i = 0;
    while (i < sorted.length) {
      const task = sorted[i];
      const base = baseTaskID(task.TaskID);
      const depth = depthCache.get(base) ?? 0;
      const isParent = base === task.TaskID;

      // Check if this is a map parent with children following
      if (isParent) {
        const children: RunTask[] = [];
        let j = i + 1;
        while (
          j < sorted.length &&
          baseTaskID(sorted[j].TaskID) === base &&
          sorted[j].TaskID !== base
        ) {
          children.push(sorted[j]);
          j++;
        }
        if (children.length > 0) {
          segments.push({ kind: "map-group", parent: task, children, depth });
          i = j;
          continue;
        }
      }
      segments.push({ kind: "single", task, depth });
      i++;
    }
    return segments;
  }, [tasks, dagDef]);

  const successCount = tasks.filter((t) => t.Status === "success").length;
  const failedCount = tasks.filter((t) => t.Status === "failed").length;
  const runningCount = tasks.filter((t) => t.Status === "running").length;
  const retryCount = tasks.filter((t) => t.Status === "up_for_retry").length;
  const totalTasks = tasks.length;
  const completedTasks = tasks.filter((t) =>
    ["success", "failed", "cancelled"].includes(t.Status)
  ).length;
  const progressPercent = totalTasks > 0 ? (completedTasks / totalTasks) * 100 : 0;

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
        subtitle={run ? `Started at ${new Date(run.CreatedAt).toLocaleString()}` : undefined}
        badge={
          run ? (
            <Badge size="sm" color={getStatusColor(run.Status)} variant="light" radius="xl">
              {getStatusLabel(run.Status)}
            </Badge>
          ) : undefined
        }
        actions={
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
                <Box mt={4}>
                  {run.Status === "running" ? (
                    <LiveElapsed startedAt={run.CreatedAt} />
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
            {/* Progress bar for running runs */}
            {run.Status === "running" && totalTasks > 0 && (
              <Box mt="sm">
                <Group justify="space-between" mb={4}>
                  <Text size="xs" c="dimmed">
                    {completedTasks} of {totalTasks} tasks complete
                  </Text>
                  <Text size="xs" c="dimmed">
                    {Math.round(progressPercent)}%
                  </Text>
                </Group>
                <Progress value={progressPercent} color="blue" size="sm" radius="xl" />
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
              return (
                <Box key={seg.task.ID}>
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
                      expanded={!!expandedMap[seg.task.ID]}
                      onToggleExpand={() => toggleTask(seg.task)}
                      onRetry={handleRetry}
                      onKill={handleKillTask}
                      taskRef={(el) => {
                        taskRefs.current[seg.task.ID] = el;
                      }}
                    />
                  </Box>
                </Box>
              );
            }

            // Map group: parent with collapsible children
            const { parent, children } = seg;
            const isGroupCollapsed = collapsedGroups[parent.TaskID] ?? true;
            const childSuccess = children.filter((c) => c.Status === "success").length;
            const childFailed = children.filter((c) => c.Status === "failed").length;
            const childRunning = children.filter((c) => c.Status === "running").length;

            return (
              <Box key={parent.ID}>
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
                    expanded={!!expandedMap[parent.ID]}
                    onToggleExpand={() => toggleTask(parent)}
                    onRetry={handleRetry}
                    onKill={handleKillTask}
                    taskRef={(el) => {
                      taskRefs.current[parent.ID] = el;
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
                        [parent.TaskID]: !isGroupCollapsed,
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
                        {children.map((child) => (
                          <TaskRow
                            key={child.ID}
                            task={child}
                            runID={id}
                            expanded={!!expandedMap[child.ID]}
                            onToggleExpand={() => toggleTask(child)}
                            onRetry={handleRetry}
                            onKill={handleKillTask}
                            taskRef={(el) => {
                              taskRefs.current[child.ID] = el;
                            }}
                          />
                        ))}
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
