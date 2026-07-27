import { describe, expect, it } from "vitest";

import { projectExecutionTopology } from ".";

describe("inspection topology metadata", () => {
  it("uses explicit mapped ancestry without parsing inspection task IDs", () => {
    const topology = projectExecutionTopology(
      [{ ID: "source" }, { ID: "mapped", MapOver: "source" }],
      [
        {
          ID: "mapped-child-attempt",
          TaskID: "opaque-child",
          Status: "running",
          InspectionKind: "mapped-child",
          ParentID: "mapped",
        },
        {
          ID: "ordinary-bracketed-attempt",
          TaskID: "mapped[99]",
          Status: "failed",
          InspectionKind: "runtime-only",
        },
      ]
    );

    expect(topology.nodes).toEqual([
      expect.objectContaining({ id: "source", kind: "definition" }),
      expect.objectContaining({
        id: "opaque-child",
        definitionId: "mapped",
        kind: "map-child",
      }),
      expect.objectContaining({ id: "mapped[99]", kind: "runtime-only" }),
    ]);
    expect(topology.edges.map(({ source, target }) => [source, target])).toEqual([
      ["source", "opaque-child"],
    ]);
  });
});
