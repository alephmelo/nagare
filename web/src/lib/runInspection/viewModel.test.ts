import { describe, expect, it } from "vitest";

import {
  InspectionAttempt,
  InspectionLogicalTask,
  RunInspection,
  inspectionAttemptDuration,
  inspectionAttemptLabel,
  inspectionCurrentTasks,
  inspectionLogStreamURL,
  projectInspectionTopology,
  segmentInspectionTasks,
  summarizeInspectionTasks,
} from ".";

function attempt(
  overrides: Partial<InspectionAttempt> & Pick<InspectionAttempt, "id" | "task_id" | "status">
): InspectionAttempt {
  return {
    attempt: 1,
    generation: 1,
    display_sequence: 1,
    command: "",
    output: "",
    item_value: null,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:01Z",
    started_at: null,
    completed_at: null,
    duration_ms: null,
    logs: { task_attempt_id: `logs-${overrides.id}`, exact: true },
    ...overrides,
  };
}

function task(
  id: string,
  current: InspectionAttempt | null,
  overrides: Partial<InspectionLogicalTask> = {}
): InspectionLogicalTask {
  return {
    id,
    kind: "definition",
    dependencies: [],
    command: "",
    attempts: current ? [current] : [],
    current_attempt: current,
    ...overrides,
  };
}

function inspection(logicalTasks: InspectionLogicalTask[]): RunInspection {
  return {
    run: {
      id: "run",
      dag_id: "dag",
      status: "running",
      exec_date: "2026-01-01T00:00:00Z",
      trigger_type: "manual",
      created_at: "2026-01-01T00:00:00Z",
      completed_at: null,
      duration_ms: null,
    },
    logical_tasks: logicalTasks,
  };
}

describe("run inspection view model", () => {
  it("uses the explicit current attempt and preserves explicit mapped ancestry", () => {
    const stale = attempt({
      id: "opaque-generation-one",
      task_id: "mapped[0]",
      status: "failed",
      generation: 1,
      display_sequence: 2,
      attempt: 2,
      item_value: "first-generation-value",
      command: "echo first-generation-value",
    });
    const current = attempt({
      id: "opaque-generation-two",
      task_id: "mapped[0]",
      status: "running",
      generation: 2,
      display_sequence: 3,
      item_value: "second-generation-value",
      command: "echo second-generation-value",
      logs: { task_attempt_id: "different-opaque-log-id", exact: true },
    });
    const mappedChild = task("mapped[0]", current, {
      kind: "mapped-child",
      definition_id: "mapped",
      parent_id: "mapped",
      attempts: [current, stale],
    });
    const value = inspection([
      task("source", null),
      task("mapped", null, { map_over: "source", dependencies: ["source"] }),
      mappedChild,
    ]);

    const topology = projectInspectionTopology(value);

    expect(inspectionCurrentTasks(value)).toEqual([mappedChild]);
    expect(topology.nodes.map((node) => node.id)).toEqual(["source", "mapped[0]"]);
    expect(topology.nodes[1]).toMatchObject({
      definitionId: "mapped",
      status: "running",
      runtimeTask: {
        ID: "opaque-generation-two",
        ParentID: "mapped",
      },
    });
    expect(current.logs).toEqual({
      task_attempt_id: "different-opaque-log-id",
      exact: true,
    });
    expect(stale).toMatchObject({
      item_value: "first-generation-value",
      command: "echo first-generation-value",
    });
    expect(current).toMatchObject({
      item_value: "second-generation-value",
      command: "echo second-generation-value",
    });
  });

  it("groups mapped children by parent metadata when topology replaces the parent", () => {
    const source = task(
      "source",
      attempt({ id: "source-attempt", task_id: "source", status: "success" })
    );
    const parent = task(
      "mapped",
      attempt({ id: "parent-attempt", task_id: "mapped", status: "success" }),
      { map_over: "source", dependencies: ["source"] }
    );
    const join = task("join", attempt({ id: "join-attempt", task_id: "join", status: "pending" }), {
      dependencies: ["mapped"],
    });
    const childTen = task(
      "mapped[10]",
      attempt({ id: "child-ten", task_id: "mapped[10]", status: "running" }),
      { kind: "mapped-child", definition_id: "mapped", parent_id: "mapped" }
    );
    const childTwo = task(
      "mapped[2]",
      attempt({ id: "child-two", task_id: "mapped[2]", status: "success" }),
      { kind: "mapped-child", definition_id: "mapped", parent_id: "mapped" }
    );
    const value = inspection([source, parent, join, childTen, childTwo]);
    const topology = projectInspectionTopology(value);

    const segments = segmentInspectionTasks(inspectionCurrentTasks(value), topology);

    expect(topology.nodes.map((node) => node.id)).toEqual([
      "source",
      "mapped[10]",
      "mapped[2]",
      "join",
    ]);
    expect(segments).toEqual([
      { kind: "single", task: source, depth: 0 },
      {
        kind: "map-group",
        parent,
        children: [childTen, childTwo],
        depth: 1,
      },
      { kind: "single", task: join, depth: 2 },
    ]);
  });

  it("does not treat an ordinary bracketed definition as a mapped child", () => {
    const bracketed = task(
      "foo[bar]",
      attempt({ id: "bracketed", task_id: "foo[bar]", status: "success" })
    );
    const value = inspection([bracketed]);
    const topology = projectInspectionTopology(value);

    expect(segmentInspectionTasks(inspectionCurrentTasks(value), topology)).toEqual([
      { kind: "single", task: bracketed, depth: 0 },
    ]);
  });

  it("summarizes only authoritative current attempts", () => {
    const value = inspection([
      task("not-started", null),
      task("done", attempt({ id: "done", task_id: "done", status: "success" })),
      task("cancelled", attempt({ id: "cancelled", task_id: "cancelled", status: "cancelled" })),
      task("retry", attempt({ id: "retry", task_id: "retry", status: "up_for_retry" })),
    ]);

    expect(summarizeInspectionTasks(inspectionCurrentTasks(value))).toEqual({
      success: 1,
      failed: 0,
      running: 0,
      retry: 1,
      total: 3,
      completed: 2,
      progress_percent: (2 / 3) * 100,
    });
  });

  it("uses display sequence for labels and explicit attempt timing before metrics", () => {
    const value = attempt({
      id: "generation-two-retry",
      task_id: "mapped[0]",
      status: "success",
      attempt: 1,
      generation: 2,
      display_sequence: 3,
      item_value: "second-generation-value",
      command: "echo second-generation-value",
      duration_ms: 42,
      metrics: {
        duration_ms: 99,
        cpu_user_ms: 1,
        cpu_system_ms: 2,
        peak_memory_bytes: 3,
        exit_code: 0,
        executor_type: "local",
      },
    });

    expect(inspectionAttemptLabel(value)).toBe("Attempt #3");
    expect(inspectionAttemptDuration(value)).toBe(42);
    expect(value).toMatchObject({
      attempt: 1,
      generation: 2,
      item_value: "second-generation-value",
      command: "echo second-generation-value",
      logs: { task_attempt_id: "logs-generation-two-retry", exact: true },
    });
  });

  it("composes exact log selection with the existing auth token query", () => {
    expect(
      inspectionLogStreamURL(
        "run",
        { task_attempt_id: "opaque-log-attempt", exact: true },
        "secret +/token"
      )
    ).toBe("/api/runs/run/tasks/opaque-log-attempt/logs?token=secret%20%2B%2Ftoken&exact=true");
    expect(
      inspectionLogStreamURL("run", {
        task_attempt_id: "opaque-log-attempt",
        exact: true,
      })
    ).toBe("/api/runs/run/tasks/opaque-log-attempt/logs?exact=true");
    expect(
      inspectionLogStreamURL(
        "run",
        { task_attempt_id: "legacy-log-attempt", exact: false },
        "secret"
      )
    ).toBe("/api/runs/run/tasks/legacy-log-attempt/logs?token=secret");
  });
});
