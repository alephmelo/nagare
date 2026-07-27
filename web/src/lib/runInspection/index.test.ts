import { describe, expect, it } from "vitest";
import { projectExecutionTopology } from "../executionTopology";
import { inspectionTopologyDefinitions, RunInspection } from ".";

describe("inspection topology projection", () => {
  it("preserves MapOver so inspection-provided mapped children replace the map parent", () => {
    const inspection = {
      run: {},
      logical_tasks: [
        {
          id: "source",
          kind: "definition",
          dependencies: [],
          command: "",
          attempts: [],
          current_attempt: null,
        },
        {
          id: "map",
          kind: "definition",
          dependencies: ["source"],
          map_over: "source",
          command: "",
          attempts: [],
          current_attempt: null,
        },
      ],
    } as unknown as RunInspection;
    const definitions = inspectionTopologyDefinitions(inspection);
    const topology = projectExecutionTopology(definitions, [
      {
        ID: "attempt",
        TaskID: "map[0]",
        InspectionKind: "mapped-child",
        ParentID: "map",
      },
    ]);

    expect(definitions[1].MapOver).toBe("source");
    expect(topology.nodes.map((node) => node.id)).toEqual(["source", "map[0]"]);
    expect(topology.nodes.find((node) => node.id === "map[0]")?.kind).toBe("map-child");
  });
});
