import { ExecutionTopology, projectExecutionTopology } from "../executionTopology";

export interface InspectionMetrics {
  duration_ms: number;
  cpu_user_ms: number;
  cpu_system_ms: number;
  peak_memory_bytes: number;
  exit_code: number;
  executor_type: string;
}

export interface InspectionLogs {
  task_attempt_id: string;
  exact: boolean;
}

export interface InspectionAttempt {
  id: string;
  task_id: string;
  attempt: number;
  generation: number;
  display_sequence: number;
  status: string;
  command: string;
  output: string;
  item_value: string | null;
  created_at: string;
  updated_at: string;
  started_at: string | null;
  completed_at: string | null;
  duration_ms: number | null;
  metrics?: InspectionMetrics;
  logs: InspectionLogs;
}

export interface InspectionLogicalTask {
  id: string;
  kind: "definition" | "mapped-child" | "runtime-only";
  definition_id?: string;
  parent_id?: string;
  dependencies: string[];
  map_over?: string;
  command: string;
  attempts: InspectionAttempt[];
  current_attempt: InspectionAttempt | null;
}

export interface RunInspection {
  run: {
    id: string;
    dag_id: string;
    status: string;
    exec_date: string;
    trigger_type: string;
    created_at: string;
    completed_at: string | null;
    duration_ms: number | null;
  };
  logical_tasks: InspectionLogicalTask[];
}

export interface InspectionTopologyDefinition {
  ID: string;
  DependsOn: string[];
  MapOver?: string;
}

export type InspectionCurrentTask = InspectionLogicalTask & {
  current_attempt: InspectionAttempt;
};

export interface InspectionTopologyRuntimeTask {
  ID: string;
  TaskID: string;
  Status: string;
  InspectionKind: InspectionLogicalTask["kind"];
  ParentID?: string;
  logicalTask: InspectionCurrentTask;
}

export type InspectionTopology = ExecutionTopology<InspectionTopologyRuntimeTask>;

export type InspectionTaskSegment =
  | { kind: "single"; task: InspectionCurrentTask; depth: number }
  | {
      kind: "map-group";
      parent: InspectionCurrentTask;
      children: InspectionCurrentTask[];
      depth: number;
    };

export interface InspectionTaskSummary {
  success: number;
  failed: number;
  running: number;
  retry: number;
  total: number;
  completed: number;
  progress_percent: number;
}

export function inspectionTopologyDefinitions(
  inspection: RunInspection
): InspectionTopologyDefinition[] {
  return inspection.logical_tasks
    .filter((task) => task.kind === "definition")
    .map((task) => ({
      ID: task.id,
      DependsOn: task.dependencies,
      MapOver: task.map_over,
    }));
}

export function inspectionCurrentTasks(inspection: RunInspection): InspectionCurrentTask[] {
  return inspection.logical_tasks.filter(
    (task): task is InspectionCurrentTask => task.current_attempt !== null
  );
}

export function projectInspectionTopology(inspection: RunInspection): InspectionTopology {
  const runtimeTasks = inspectionCurrentTasks(inspection).map((task) => ({
    ID: task.current_attempt.id,
    TaskID: task.id,
    Status: task.current_attempt.status,
    InspectionKind: task.kind,
    ParentID: task.parent_id,
    logicalTask: task,
  }));
  return projectExecutionTopology(inspectionTopologyDefinitions(inspection), runtimeTasks);
}

export function segmentInspectionTasks(
  tasks: readonly InspectionCurrentTask[],
  topology: InspectionTopology
): InspectionTaskSegment[] {
  const sourceOrder = new Map(tasks.map((task, index) => [task.id, index]));
  const topologyOrder = new Map(topology.nodes.map((node, index) => [node.id, index]));
  const stageOf = new Map<string, number>();
  topology.stages.forEach((stage, depth) => {
    stage.forEach((taskID) => stageOf.set(taskID, depth));
  });

  const taskByID = new Map(tasks.map((task) => [task.id, task]));
  const childrenByParent = new Map<string, InspectionCurrentTask[]>();
  for (const task of tasks) {
    if (task.kind !== "mapped-child" || !task.parent_id) continue;
    const group = childrenByParent.get(task.parent_id) ?? [];
    group.push(task);
    childrenByParent.set(task.parent_id, group);
  }
  for (const children of childrenByParent.values()) {
    children.sort(
      (a, b) =>
        (topologyOrder.get(a.id) ?? sourceOrder.get(a.id) ?? Number.MAX_SAFE_INTEGER) -
        (topologyOrder.get(b.id) ?? sourceOrder.get(b.id) ?? Number.MAX_SAFE_INTEGER)
    );
  }

  const rank = (ids: readonly string[], fallbackID: string) => {
    const renderedIDs = ids.filter((id) => topologyOrder.has(id));
    const rankedIDs = renderedIDs.length > 0 ? renderedIDs : [fallbackID];
    return {
      depth: Math.min(...rankedIDs.map((id) => stageOf.get(id) ?? 0)),
      order: Math.min(
        ...rankedIDs.map(
          (id) => topologyOrder.get(id) ?? sourceOrder.get(id) ?? Number.MAX_SAFE_INTEGER
        )
      ),
      source: sourceOrder.get(fallbackID) ?? Number.MAX_SAFE_INTEGER,
    };
  };

  const ranked: Array<InspectionTaskSegment & { order: number; source: number }> = [];
  for (const task of tasks) {
    if (task.kind === "mapped-child" && task.parent_id && taskByID.has(task.parent_id)) {
      continue;
    }
    const children = childrenByParent.get(task.id) ?? [];
    const taskRank = rank(
      children.length > 0 ? children.map((child) => child.id) : [task.id],
      task.id
    );
    if (children.length > 0) {
      ranked.push({
        kind: "map-group",
        parent: task,
        children,
        ...taskRank,
      });
    } else {
      ranked.push({ kind: "single", task, ...taskRank });
    }
  }

  ranked.sort((a, b) => a.depth - b.depth || a.order - b.order || a.source - b.source);
  return ranked.map(
    (segment): InspectionTaskSegment =>
      segment.kind === "single"
        ? {
            kind: segment.kind,
            task: segment.task,
            depth: segment.depth,
          }
        : {
            kind: segment.kind,
            parent: segment.parent,
            children: segment.children,
            depth: segment.depth,
          }
  );
}

export function summarizeInspectionTasks(
  tasks: readonly InspectionCurrentTask[]
): InspectionTaskSummary {
  const statuses = tasks.map((task) => task.current_attempt.status);
  const completed = statuses.filter((status) =>
    ["success", "failed", "cancelled"].includes(status)
  ).length;
  return {
    success: statuses.filter((status) => status === "success").length,
    failed: statuses.filter((status) => status === "failed").length,
    running: statuses.filter((status) => status === "running").length,
    retry: statuses.filter((status) => status === "up_for_retry").length,
    total: statuses.length,
    completed,
    progress_percent: statuses.length > 0 ? (completed / statuses.length) * 100 : 0,
  };
}

export function inspectionAttemptLabel(attempt: InspectionAttempt): string {
  return `Attempt #${attempt.display_sequence}`;
}

export function inspectionAttemptDuration(attempt: InspectionAttempt): number | null {
  return attempt.duration_ms ?? attempt.metrics?.duration_ms ?? null;
}

export function inspectionLogStreamURL(
  runID: string,
  logs: InspectionLogs,
  authToken?: string | null
): string {
  const query: string[] = [];
  if (authToken) query.push(`token=${encodeURIComponent(authToken)}`);
  if (logs.exact) query.push("exact=true");
  const suffix = query.length > 0 ? `?${query.join("&")}` : "";
  return `/api/runs/${runID}/tasks/${logs.task_attempt_id}/logs${suffix}`;
}

export async function fetchRunInspection(
  runID: string,
  fetcher: typeof fetch
): Promise<RunInspection> {
  const response = await fetcher(`/api/runs/${runID}/inspection`);
  if (!response.ok) throw new Error(`run inspection failed: ${response.status}`);
  return response.json();
}
