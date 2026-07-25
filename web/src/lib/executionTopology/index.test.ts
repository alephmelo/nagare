import { describe, expect, it } from "vitest";

import { projectExecutionTopology } from "./index";

type Definition = {
  ID: string;
  DependsOn?: string[] | null;
  MapOver?: string;
};

type RuntimeTask = {
  ID: string;
  TaskID: string;
  Status: string;
};

function project(definitions: Definition[], runtimeTasks: RuntimeTask[] = []) {
  return projectExecutionTopology(definitions, runtimeTasks);
}

function structureOf(topology: ReturnType<typeof projectExecutionTopology>) {
  return {
    nodeIds: topology.nodes.map((node) => node.id),
    edgeIds: topology.edges.map((edge) => edge.id),
    edges: topology.edges.map(({ source, target }) => ({ source, target })),
    stages: topology.stages.map((stage) => [...stage]),
  };
}

describe("projectExecutionTopology", () => {
  it("projects ordinary dependencies into deterministic stages and edges", () => {
    const definitions = [
      { ID: "prepare" },
      { ID: "lint", DependsOn: ["prepare"] },
      { ID: "test", DependsOn: ["prepare"] },
      { ID: "publish", DependsOn: ["lint", "test"] },
    ];

    const first = project(definitions);
    const second = project(definitions.map((definition) => ({ ...definition })));

    expect(first.stages).toEqual([["prepare"], ["lint", "test"], ["publish"]]);
    expect(first.edges.map(({ source, target }) => [source, target])).toEqual([
      ["prepare", "lint"],
      ["prepare", "test"],
      ["lint", "publish"],
      ["test", "publish"],
    ]);
    expect(structureOf(second)).toEqual(structureOf(first));
  });

  it("treats MapOver as a dependency even when DependsOn does not repeat it", () => {
    const topology = project([
      { ID: "discover" },
      { ID: "process", DependsOn: [], MapOver: "discover" },
      { ID: "report", DependsOn: ["process"] },
    ]);

    expect(topology.stages).toEqual([["discover"], ["process"], ["report"]]);
    expect(topology.edges.map(({ source, target }) => [source, target])).toEqual([
      ["discover", "process"],
      ["process", "report"],
    ]);
  });

  it("deduplicates a MapOver relationship repeated in DependsOn", () => {
    const topology = project([
      { ID: "discover" },
      { ID: "process", DependsOn: ["discover"], MapOver: "discover" },
    ]);

    expect(topology.edges).toHaveLength(1);
    expect(topology.edges[0]).toMatchObject({ source: "discover", target: "process" });
  });

  it("keeps a static mapped definition visible before runtime children exist", () => {
    const topology = project([
      { ID: "discover" },
      { ID: "process", MapOver: "discover" },
      { ID: "join", DependsOn: ["process"] },
    ]);

    expect(topology.nodes.map((node) => node.id)).toEqual(["discover", "process", "join"]);
    expect(topology.stages).toEqual([["discover"], ["process"], ["join"]]);
  });

  it("expands dynamic map children in numeric index order and fans edges through them", () => {
    const definitions = [
      { ID: "discover" },
      { ID: "process", MapOver: "discover" },
      { ID: "join", DependsOn: ["process"] },
    ];
    const runtimeTasks = [
      { ID: "runtime-10", TaskID: "process[10]", Status: "success" },
      { ID: "runtime-source", TaskID: "discover", Status: "success" },
      { ID: "runtime-2", TaskID: "process[2]", Status: "running" },
      { ID: "runtime-1", TaskID: "process[1]", Status: "success" },
    ];

    const topology = project(definitions, runtimeTasks);

    expect(topology.nodes.map((node) => node.id)).toEqual([
      "discover",
      "process[1]",
      "process[2]",
      "process[10]",
      "join",
    ]);
    expect(topology.stages).toEqual([
      ["discover"],
      ["process[1]", "process[2]", "process[10]"],
      ["join"],
    ]);
    expect(topology.edges.map(({ source, target }) => [source, target])).toEqual([
      ["discover", "process[1]"],
      ["discover", "process[2]"],
      ["discover", "process[10]"],
      ["process[1]", "join"],
      ["process[2]", "join"],
      ["process[10]", "join"],
    ]);
  });

  it("retains definition nodes for an empty run and runtime-only tasks for inspection", () => {
    const definitions = [{ ID: "defined" }, { ID: "consumer", DependsOn: ["defined"] }];

    const emptyRun = project(definitions);
    const withRuntimeOnly = project(definitions, [
      { ID: "runtime-legacy", TaskID: "legacy", Status: "failed" },
    ]);

    expect(emptyRun.nodes.map((node) => node.id)).toEqual(["defined", "consumer"]);
    expect(withRuntimeOnly.nodes.map((node) => node.id)).toContain("legacy");
    expect(withRuntimeOnly.nodes.find((node) => node.id === "legacy")).toMatchObject({
      kind: "runtime-only",
      status: "failed",
    });
  });

  it("stages a runtime-only dependency before its definition consumer", () => {
    const topology = project(
      [{ ID: "consume", DependsOn: ["legacy"] }],
      [
        { ID: "runtime-legacy", TaskID: "legacy", Status: "success" },
        { ID: "runtime-consume", TaskID: "consume", Status: "pending" },
      ]
    );

    expect(topology.edges.map(({ source, target }) => [source, target])).toContainEqual([
      "legacy",
      "consume",
    ]);
    expect(topology.stages).toEqual([["legacy"], ["consume"]]);
  });

  it("gives definition and runtime overlays identical topology semantics", () => {
    const definitions = [
      { ID: "source" },
      { ID: "mapped", MapOver: "source" },
      { ID: "join", DependsOn: ["mapped"] },
    ];
    const definitionOnly = project(definitions);
    const runOverlay = project(
      definitions,
      definitions.map((definition, index) => ({
        ID: `runtime-${index}`,
        TaskID: definition.ID,
        Status: index === 0 ? "success" : "pending",
      }))
    );

    expect(structureOf(runOverlay)).toEqual(structureOf(definitionOnly));
  });

  it("preserves structural identity and order when only statuses change", () => {
    const definitions = [
      { ID: "start" },
      { ID: "left", DependsOn: ["start"] },
      { ID: "right", DependsOn: ["start"] },
    ];
    const pending = project(
      definitions,
      definitions.map((definition, index) => ({
        ID: `runtime-${index}`,
        TaskID: definition.ID,
        Status: "pending",
      }))
    );
    const updated = project(
      definitions,
      definitions.map((definition, index) => ({
        ID: `runtime-${index}`,
        TaskID: definition.ID,
        Status: index === 0 ? "success" : index === 1 ? "running" : "failed",
      }))
    );

    expect(structureOf(updated)).toEqual(structureOf(pending));
    expect(updated.nodes.map((node) => node.status)).toEqual(["success", "running", "failed"]);
  });
});
