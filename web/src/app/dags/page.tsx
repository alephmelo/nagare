"use client";

import { useEffect, useState, useMemo } from "react";
import { useSearchParams } from "next/navigation";
import {
  Title,
  Card,
  Table,
  Badge,
  Text,
  Group,
  Button,
  Tabs,
  Skeleton,
  Pagination,
  Alert
} from "@mantine/core";
import { IconArrowLeft, IconAlertCircle, IconPlayerPlay } from "@tabler/icons-react";
import { useRouter } from "next/navigation";
import { ReactFlow, Controls, Background, useNodesState, useEdgesState, Position, MarkerType, Node, Edge } from "@xyflow/react";
import '@xyflow/react/dist/style.css';
import dagre from "dagre";

// Types
interface TaskDef {
  ID: string;
  Type: string;
  Command: string;
  DependsOn: string[];
}

interface Dag {
  ID: string;
  Schedule: string;
  Description: string;
  Tasks: TaskDef[];
}

interface Run {
  ID: string;
  DAGID: string;
  Status: string;
  ExecDate: string;
  UpdatedAt: string;
  CreatedAt: string;
}

// Dagre Layout config
const dagreGraph = new dagre.graphlib.Graph();
dagreGraph.setDefaultEdgeLabel(() => ({}));

const nodeWidth = 280;
const nodeHeight = 60;

const getLayoutedElements = (nodes: Node[], edges: Edge[], direction = 'TB') => {
  const isHorizontal = direction === 'LR';
  dagreGraph.setGraph({ rankdir: direction });

  nodes.forEach((node) => {
    dagreGraph.setNode(node.id, { width: nodeWidth, height: nodeHeight });
  });

  edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  dagre.layout(dagreGraph);

  nodes.forEach((node) => {
    const nodeWithPosition = dagreGraph.node(node.id);
    node.targetPosition = isHorizontal ? Position.Left : Position.Top;
    node.sourcePosition = isHorizontal ? Position.Right : Position.Bottom;

    // Shift to center the node properly
    node.position = {
      x: nodeWithPosition.x - nodeWidth / 2,
      y: nodeWithPosition.y - nodeHeight / 2,
    };

    return node;
  });

  return { nodes, edges };
};

export default function DagDetails() {
  const searchParams = useSearchParams();
  const id = searchParams.get("id");
  const router = useRouter();

  const [dag, setDag] = useState<Dag | null>(null);
  const [runs, setRuns] = useState<Run[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [triggering, setTriggering] = useState(false);

  // Pagination bounds
  const [page, setPage] = useState(1);
  const [totalRuns, setTotalRuns] = useState(0);
  const limit = 10;

  // React Flow strict local states
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);

  useEffect(() => {
    if (!id) return;
    
    // Initial fetch of DAG specific definitions
    const initializeView = async () => {
      setLoading(true);
      try {
        const dagsRes = await fetch("/api/dags");
        const allDags: Dag[] = await dagsRes.json();
        
        const targetDag = allDags.find(d => d.ID === id);
        if (!targetDag) {
          setError(`DAG ${id} not found in memory`);
          return;
        }

        setDag(targetDag);

        // Build react flow nodes off schema dynamically
        const initialNodes = targetDag.Tasks?.map(task => ({
          id: task.ID,
          data: { label: task.ID },
          position: { x: 0, y: 0 },
          style: {
            background: 'var(--mantine-color-dark-6)',
            color: 'white',
            border: '1px solid var(--mantine-color-blue-filled)',
            borderRadius: '8px',
            padding: '10px 15px',
            fontSize: '12px',
            fontWeight: 600,
            width: nodeWidth,
            wordBreak: 'break-word' as const,
            whiteSpace: 'pre-wrap' as const,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            textAlign: 'center' as const
          }
        })) || [];

        const initialEdges: Edge[] = [];
        targetDag.Tasks?.forEach(task => {
          if (task.DependsOn && task.DependsOn.length > 0) {
            task.DependsOn.forEach(dep => {
              initialEdges.push({
                id: `e-${dep}-${task.ID}`,
                source: dep,
                target: task.ID,
                animated: true,
                style: { stroke: 'var(--mantine-color-blue-filled)' },
                markerEnd: {
                    type: MarkerType.ArrowClosed,
                    width: 20,
                    height: 20,
                    color: 'var(--mantine-color-blue-filled)',
                }
              });
            });
          }
        });

        const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(
          initialNodes,
          initialEdges,
          'TB' // top-down direction
        );

        setNodes(layoutedNodes);
        setEdges(layoutedEdges);
          
      } catch (err) {
        console.error(err);
        setError("Failed to load DAG schema from database.");
      } finally {
        setLoading(false);
      }
    };

    initializeView();
  }, [id, setNodes, setEdges]);

  useEffect(() => {
    if (!id) return;

    // Periodic fetching runs list to sync paginated data
    const fetchRuns = async () => {
      try {
        const runsRes = await fetch(`/api/runs?page=${page}&limit=${limit}&dag_id=${id}`);
        const runsData = await runsRes.json();
        setRuns(runsData.data || []);
        setTotalRuns(runsData.total || 0);
      } catch(err) {
        console.error("Failed to query runs", err);
      }
    }

    fetchRuns();
    const interval = setInterval(fetchRuns, 5000);
    return () => clearInterval(interval);

  }, [id, page]);

  const handleTrigger = async () => {
    if (!id) return;
    setTriggering(true);
    try {
      const res = await fetch(`/api/dags/${id}/runs`, { method: "POST" });
      if (res.ok) {
        setPage(1); // Reset to first page so they see it
      } else {
        alert("Failed to manual trigger DAG");
      }
    } catch (err) {
      console.error(err);
    } finally {
      setTriggering(false);
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case "success": return "green";
      case "failed": return "red";
      case "running": return "blue";
      case "queued": return "yellow";
      default: return "gray";
    }
  };

  if (!id) {
    return (
      <Alert color="red" title="Error">
        No DAG ID provided in URL parameters.
      </Alert>
    );
  }

  return (
    <>
      <Group justify="space-between" mb="xs">
        <Group>
          <Button variant="subtle" leftSection={<IconArrowLeft size={16} />} onClick={() => router.push('/')} color="gray">
            Back
          </Button>
          <Title order={2}>{id}</Title>
          {dag && <Badge variant="light" color="cyan" size="lg">{dag.Schedule}</Badge>}
        </Group>
        {dag && (
          <Button 
            leftSection={<IconPlayerPlay size={16} />} 
            color="blue" 
            onClick={handleTrigger}
            loading={triggering}
          >
            Trigger Pipeline
          </Button>
        )}
      </Group>
      
      {dag && (
        <Text c="dimmed" mb="xl">
          {dag.Description}
        </Text>
      )}

      {error ? (
        <Alert variant="light" color="red" title="DAG Unloadable" icon={<IconAlertCircle />}>
          {error}
        </Alert>
      ) : loading ? (
        <Skeleton height={400} />
      ) : (
        <Tabs defaultValue="graph" variant="outline">
          <Tabs.List>
            <Tabs.Tab value="graph">Graph View</Tabs.Tab>
            <Tabs.Tab value="table">Table View (Runs)</Tabs.Tab>
          </Tabs.List>

          <Tabs.Panel value="graph" pt="xl">
            <Card shadow="sm" radius="md" withBorder style={{ height: '60vh', minHeight: '500px' }} p="0">
                <ReactFlow
                  nodes={nodes}
                  edges={edges}
                  onNodesChange={onNodesChange}
                  onEdgesChange={onEdgesChange}
                  fitView
                  attributionPosition="bottom-right"
                >
                  <Background color="#ccc" gap={16} />
                  <Controls />
                </ReactFlow>
            </Card>
          </Tabs.Panel>

          <Tabs.Panel value="table" pt="xl">
             <Card shadow="sm" radius="md" withBorder padding="0" style={{ overflow: "hidden" }}>
              <Table.ScrollContainer minWidth={800}>
                <Table verticalSpacing="sm" striped highlightOnHover>
                  <Table.Thead>
                    <Table.Tr>
                      <Table.Th>Run ID</Table.Th>
                      <Table.Th>Status</Table.Th>
                      <Table.Th>Execution Date</Table.Th>
                      <Table.Th>Elapsed Time</Table.Th>
                    </Table.Tr>
                  </Table.Thead>
                  <Table.Tbody>
                    {runs?.map((run) => (
                      <Table.Tr key={run.ID} onClick={() => router.push(`/runs/?id=${run.ID}`)} style={{ cursor: "pointer" }}>
                        <Table.Td>
                          <Text size="sm" fw={500} c="cyan">{run.ID}</Text>
                        </Table.Td>
                        <Table.Td>
                          <Badge color={getStatusColor(run.Status)} variant="light" size="sm">
                            {run.Status.toUpperCase()}
                          </Badge>
                        </Table.Td>
                        <Table.Td>
                          <Text size="sm">{new Date(run.ExecDate).toLocaleString()}</Text>
                        </Table.Td>
                        <Table.Td>
                          <Text size="sm" c="dimmed">
                            {run.UpdatedAt && run.CreatedAt 
                              ? `${Math.max(1, Math.floor((new Date(run.UpdatedAt).getTime() - new Date(run.CreatedAt).getTime()) / 1000))}s`
                              : "-"}
                          </Text>
                        </Table.Td>
                      </Table.Tr>
                    ))}
                    {(!runs || runs.length === 0) && (
                        <Table.Tr>
                          <Table.Td colSpan={5} align="center" py="xl">
                            <Text c="dimmed">No past runs found for this DAG.</Text>
                          </Table.Td>
                        </Table.Tr>
                    )}
                  </Table.Tbody>
                </Table>
              </Table.ScrollContainer>
              {totalRuns > limit && (
                <Group justify="center" p="md" style={{ borderTop: "1px solid var(--mantine-color-default-border)" }}>
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
          </Tabs.Panel>
        </Tabs>
      )}
    </>
  );
}
