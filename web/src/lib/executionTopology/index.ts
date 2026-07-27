export interface ExecutionDefinition {
  ID: string;
  DependsOn?: string[] | null;
  MapOver?: string | null;
}

export interface ExecutionRuntimeTask {
  ID: string;
  TaskID: string;
  Status?: string;
  InspectionKind?: "definition" | "mapped-child" | "runtime-only";
  ParentID?: string;
}

export interface ProjectedNode<T extends ExecutionRuntimeTask = ExecutionRuntimeTask> {
  id: string;
  definitionId?: string;
  kind: "definition" | "map-child" | "runtime-only";
  status?: string;
  runtimeTask?: T;
}

export interface ProjectedEdge {
  id: string;
  source: string;
  target: string;
}

export interface ExecutionTopology<T extends ExecutionRuntimeTask = ExecutionRuntimeTask> {
  nodes: ProjectedNode<T>[];
  edges: ProjectedEdge[];
  stages: string[][];
}

const childMatch = (id: string) => id.match(/^(.*)\[(\d+)\]$/);

export function projectExecutionTopology<T extends ExecutionRuntimeTask>(
  definitions: readonly ExecutionDefinition[],
  runtimeTasks: readonly T[] = []
): ExecutionTopology<T> {
  const mappedDefinitionIds = new Set(
    definitions.filter((definition) => definition.MapOver).map((definition) => definition.ID)
  );
  const latestRuntime = new Map<string, T>();
  const runtimeOrder = new Map<string, number>();
  runtimeTasks.forEach((task, index) => runtimeOrder.set(task.TaskID, index));
  for (const task of runtimeTasks) latestRuntime.set(task.TaskID, task);

  const children = new Map<string, T[]>();
  for (const task of latestRuntime.values()) {
    const legacyMatch = task.InspectionKind === undefined ? childMatch(task.TaskID) : undefined;
    const parentID =
      task.InspectionKind === "mapped-child"
        ? task.ParentID
        : task.InspectionKind === undefined &&
            legacyMatch &&
            mappedDefinitionIds.has(legacyMatch[1])
          ? legacyMatch[1]
          : undefined;
    if (!parentID || !mappedDefinitionIds.has(parentID)) continue;
    const group = children.get(parentID) ?? [];
    group.push(task);
    children.set(parentID, group);
  }
  for (const group of children.values()) {
    group.sort((a, b) => {
      if (a.InspectionKind === undefined && b.InspectionKind === undefined) {
        const ai = Number(childMatch(a.TaskID)?.[2]);
        const bi = Number(childMatch(b.TaskID)?.[2]);
        return ai - bi || a.TaskID.localeCompare(b.TaskID);
      }
      return (runtimeOrder.get(a.TaskID) ?? 0) - (runtimeOrder.get(b.TaskID) ?? 0);
    });
  }

  const nodes: ProjectedNode<T>[] = [];
  const renderedIds = new Set<string>();
  for (const definition of definitions) {
    const mappedChildren = children.get(definition.ID);
    if (mappedChildren?.length) {
      renderedIds.add(definition.ID);
      for (const task of mappedChildren) {
        nodes.push({
          id: task.TaskID,
          definitionId: definition.ID,
          kind: "map-child",
          status: task.Status,
          runtimeTask: task,
        });
        renderedIds.add(task.TaskID);
      }
    } else {
      const task = latestRuntime.get(definition.ID);
      nodes.push({
        id: definition.ID,
        definitionId: definition.ID,
        kind: "definition",
        status: task?.Status,
        runtimeTask: task,
      });
      renderedIds.add(definition.ID);
    }
  }
  for (const task of latestRuntime.values()) {
    if (renderedIds.has(task.TaskID)) continue;
    if (task.InspectionKind === undefined) {
      const match = childMatch(task.TaskID);
      if (match && mappedDefinitionIds.has(match[1])) continue;
    }
    nodes.push({
      id: task.TaskID,
      kind: "runtime-only",
      status: task.Status,
      runtimeTask: task,
    });
    renderedIds.add(task.TaskID);
  }

  const expand = (id: string) => children.get(id)?.map((task) => task.TaskID) ?? [id];
  const edgeKeys = new Set<string>();
  const edges: ProjectedEdge[] = [];
  for (const definition of definitions) {
    const dependencies = [...(definition.DependsOn ?? [])];
    if (definition.MapOver) dependencies.push(definition.MapOver);
    for (const dependency of [...new Set(dependencies)]) {
      for (const source of expand(dependency)) {
        for (const target of expand(definition.ID)) {
          if (!renderedIds.has(source) || !renderedIds.has(target) || source === target) continue;
          const key = `${source}->${target}`;
          if (edgeKeys.has(key)) continue;
          edgeKeys.add(key);
          edges.push({ id: `e-${source}-${target}`, source, target });
        }
      }
    }
  }

  const nodeOrder = new Map(nodes.map((node, index) => [node.id, index]));
  const incoming = new Map(nodes.map((node) => [node.id, 0]));
  const outgoing = new Map(nodes.map((node) => [node.id, [] as string[]]));
  for (const edge of edges) {
    incoming.set(edge.target, (incoming.get(edge.target) ?? 0) + 1);
    outgoing.get(edge.source)?.push(edge.target);
  }
  const stages: string[][] = [];
  const remaining = new Set(nodes.map((node) => node.id));
  while (remaining.size) {
    let stage = [...remaining].filter((id) => incoming.get(id) === 0);
    if (!stage.length)
      stage = [[...remaining].sort((a, b) => nodeOrder.get(a)! - nodeOrder.get(b)!)[0]];
    stage.sort((a, b) => nodeOrder.get(a)! - nodeOrder.get(b)!);
    stages.push(stage);
    for (const id of stage) {
      remaining.delete(id);
      for (const target of outgoing.get(id) ?? []) {
        incoming.set(target, (incoming.get(target) ?? 0) - 1);
      }
    }
  }

  return { nodes, edges, stages };
}
