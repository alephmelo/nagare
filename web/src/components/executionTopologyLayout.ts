import { Position, type Node } from "@xyflow/react";

export function layoutTopologyStages<T extends Node>(
  nodes: readonly T[],
  stages: readonly (readonly string[])[],
  width: number,
  height: number,
  columnGap = 20,
  rowGap = 40
): T[] {
  const byId = new Map(nodes.map((node) => [node.id, node]));
  return stages.flatMap((stage, row) =>
    stage.flatMap((id, column) => {
      const node = byId.get(id);
      if (!node) return [];
      return [
        {
          ...node,
          position: { x: column * (width + columnGap), y: row * (height + rowGap) },
          targetPosition: Position.Top,
          sourcePosition: Position.Bottom,
        },
      ];
    })
  );
}
