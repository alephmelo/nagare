"use client";

import { Suspense, useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";
import { apiFetch } from "../../lib/apiFetch";
import { useVisibilityPoll } from "../../lib/useVisibilityPoll";
import {
  Title,
  Card,
  Table,
  Badge,
  Text,
  Group,
  Button,
  Grid,
  Skeleton,
  Pagination,
  Alert,
  Menu,
  UnstyledButton,
  ActionIcon,
  Tooltip,
  List,
  Tabs,
  ScrollArea,
  Switch,
  useMantineColorScheme,
} from "@mantine/core";
import {
  IconArrowLeft,
  IconAlertCircle,
  IconPlayerPlay,
  IconRobot,
  IconUser,
  IconFilter,
  IconCheck,
  IconPlayerStop,
  IconActivity,
  IconRefresh,
} from "@tabler/icons-react";
import { useRouter } from "next/navigation";
import {
  ReactFlow,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  Position,
  MarkerType,
  Node,
  Edge,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import dagre from "dagre";
import { notifications } from "@mantine/notifications";
import SyntaxHighlighter from "react-syntax-highlighter";
import { atomOneDark, atomOneLight } from "react-syntax-highlighter/dist/esm/styles/hljs";

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
  Paused: boolean;
}

interface Run {
  ID: string;
  DAGID: string;
  Status: string;
  ExecDate: string;
  TriggerType: string;
  CreatedAt: string;
  CompletedAt?: string;
}

// Dagre Layout config
const dagreGraph = new dagre.graphlib.Graph();
dagreGraph.setDefaultEdgeLabel(() => ({}));

const nodeWidth = 280;
const nodeHeight = 60;

const getLayoutedElements = (nodes: Node[], edges: Edge[], direction = "TB") => {
  const isHorizontal = direction === "LR";
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

// ---------------------------------------------------------------------------
// DAG List View (no ?id param)
// ---------------------------------------------------------------------------
function DagListContent() {
  const [dags, setDags] = useState<Dag[]>([]);
  const [dagErrors, setDagErrors] = useState<Record<string, string>>({});
  const [triggering, setTriggering] = useState<Record<string, boolean>>({});
  const [pausing, setPausing] = useState<Record<string, boolean>>({});
  const [loading, setLoading] = useState(true);
  const router = useRouter();

  const fetchData = async () => {
    try {
      setLoading(true);
      const [dagsRes, errorsRes] = await Promise.all([
        apiFetch("/api/dags"),
        apiFetch("/api/dags/errors"),
      ]);
      if (dagsRes.ok) setDags(await dagsRes.json());
      if (errorsRes.ok) setDagErrors((await errorsRes.json()) || {});
    } catch (err) {
      console.error("Failed to fetch DAGs", err);
    } finally {
      setLoading(false);
    }
  };

  useVisibilityPoll(fetchData, 5000);

  const handleTrigger = async (dagID: string) => {
    setTriggering((prev) => ({ ...prev, [dagID]: true }));
    try {
      const res = await apiFetch(`/api/dags/${dagID}/runs`, { method: "POST" });
      if (res.ok) {
        notifications.show({
          title: "Pipeline Triggered",
          message: `Successfully enqueued a fresh manual run for ${dagID}.`,
          color: "green",
        });
      }
    } catch (err) {
      console.error(err);
    } finally {
      setTriggering((prev) => ({ ...prev, [dagID]: false }));
    }
  };

  const handleTogglePause = async (dagID: string, currentlyPaused: boolean) => {
    setPausing((prev) => ({ ...prev, [dagID]: true }));
    // Optimistic update
    setDags((prev) => prev.map((d) => (d.ID === dagID ? { ...d, Paused: !currentlyPaused } : d)));
    try {
      const action = currentlyPaused ? "activate" : "pause";
      const res = await apiFetch(`/api/dags/${dagID}/${action}`, { method: "POST" });
      if (!res.ok) {
        // Revert optimistic update on failure
        setDags((prev) =>
          prev.map((d) => (d.ID === dagID ? { ...d, Paused: currentlyPaused } : d))
        );
        notifications.show({
          title: "Action Failed",
          message: `Could not ${action} ${dagID}.`,
          color: "red",
        });
      } else {
        notifications.show({
          title: currentlyPaused ? "Pipeline Activated" : "Pipeline Paused",
          message: currentlyPaused
            ? `${dagID} is now active and will run on schedule.`
            : `${dagID} is paused. Scheduled runs are suspended.`,
          color: currentlyPaused ? "green" : "yellow",
        });
      }
    } catch (err) {
      console.error(err);
      // Revert on error
      setDags((prev) => prev.map((d) => (d.ID === dagID ? { ...d, Paused: currentlyPaused } : d)));
    } finally {
      setPausing((prev) => ({ ...prev, [dagID]: false }));
    }
  };

  return (
    <>
      <Group justify="space-between" mb="xl">
        <Title order={2}>DAGs</Title>
        <Button leftSection={<IconRefresh size={16} />} variant="light" onClick={fetchData}>
          Refresh
        </Button>
      </Group>

      {Object.keys(dagErrors).length > 0 && (
        <Alert
          variant="light"
          color="red"
          title="DAG Validation Errors"
          icon={<IconAlertCircle />}
          mb="xl"
        >
          <Text size="sm" mb="xs">
            Problematic DAG configurations:
          </Text>
          <List size="sm" spacing="xs">
            {Object.entries(dagErrors).map(([file, err]) => (
              <List.Item key={file}>
                <strong>{file}</strong>:{" "}
                <Text span c="dimmed" fs="italic">
                  {err}
                </Text>
              </List.Item>
            ))}
          </List>
        </Alert>
      )}

      {loading && dags.length === 0 ? (
        <Skeleton height={300} radius="md" />
      ) : (
        <Card padding="0" style={{ overflow: "hidden" }}>
          <Table.ScrollContainer minWidth={600}>
            <Table verticalSpacing="sm" horizontalSpacing="md" striped highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th style={{ borderBottom: "2px solid var(--border-color)" }}>
                    <Text size="sm" fw={700}>
                      Pipeline
                    </Text>
                  </Table.Th>
                  <Table.Th style={{ borderBottom: "2px solid var(--border-color)" }}>
                    <Text size="sm" fw={700}>
                      Schedule
                    </Text>
                  </Table.Th>
                  <Table.Th style={{ borderBottom: "2px solid var(--border-color)" }}>
                    <Text size="sm" fw={700}>
                      Tasks
                    </Text>
                  </Table.Th>
                  <Table.Th
                    style={{
                      borderBottom: "2px solid var(--border-color)",
                      width: "110px",
                      textAlign: "right",
                    }}
                  >
                    <Text size="sm" fw={700}>
                      Actions
                    </Text>
                  </Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {dags?.map((dag) => (
                  <Table.Tr
                    key={dag.ID}
                    onClick={() => router.push(`/dags?id=${dag.ID}`)}
                    style={{ cursor: "pointer", opacity: dag.Paused ? 0.6 : 1 }}
                  >
                    <Table.Td>
                      <Text fw={600} size="sm">
                        {dag.ID}
                      </Text>
                      <Text
                        size="xs"
                        c="dimmed"
                        mt={2}
                        style={{
                          maxWidth: "500px",
                          whiteSpace: "nowrap",
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                        }}
                      >
                        {dag.Description}
                      </Text>
                    </Table.Td>
                    <Table.Td>
                      {dag.Paused ? (
                        <Badge variant="light" color="yellow" size="sm" radius="sm">
                          Paused
                        </Badge>
                      ) : (
                        <Badge variant="light" color="blue" size="sm" radius="sm">
                          {dag.Schedule}
                        </Badge>
                      )}
                    </Table.Td>
                    <Table.Td>
                      <Text size="sm" c="dimmed">
                        {dag.Tasks?.length ?? 0} task{dag.Tasks?.length !== 1 ? "s" : ""}
                      </Text>
                    </Table.Td>
                    <Table.Td align="right">
                      <Group gap="xs" justify="flex-end" wrap="nowrap">
                        <Tooltip
                          label={dag.Paused ? "Activate pipeline" : "Pause pipeline"}
                          position="left"
                        >
                          <span
                            onClick={(e) => {
                              e.stopPropagation();
                              if (!pausing[dag.ID]) handleTogglePause(dag.ID, dag.Paused);
                            }}
                            style={{
                              display: "inline-flex",
                              alignItems: "center",
                              cursor: "pointer",
                            }}
                          >
                            <Switch
                              checked={!dag.Paused}
                              disabled={pausing[dag.ID]}
                              size="sm"
                              color="blue"
                              readOnly
                            />
                          </span>
                        </Tooltip>
                        <Tooltip label="Trigger Pipeline" position="left">
                          <ActionIcon
                            variant="light"
                            color="blue"
                            onClick={(e) => {
                              e.stopPropagation();
                              handleTrigger(dag.ID);
                            }}
                            loading={triggering[dag.ID]}
                            disabled={triggering[dag.ID]}
                          >
                            <IconPlayerPlay size={16} />
                          </ActionIcon>
                        </Tooltip>
                      </Group>
                    </Table.Td>
                  </Table.Tr>
                ))}
                {(!dags || dags.length === 0) && (
                  <Table.Tr>
                    <Table.Td colSpan={4}>
                      <Text c="dimmed" ta="center" py="md">
                        No pipelines loaded.
                      </Text>
                    </Table.Td>
                  </Table.Tr>
                )}
              </Table.Tbody>
            </Table>
          </Table.ScrollContainer>
        </Card>
      )}
    </>
  );
}

// ---------------------------------------------------------------------------
// DAG Detail View (?id=... param present)
// ---------------------------------------------------------------------------
function DagDetailsContent() {
  const searchParams = useSearchParams();
  const id = searchParams.get("id");
  const router = useRouter();
  const { colorScheme } = useMantineColorScheme();

  const [dag, setDag] = useState<Dag | null>(null);
  const [runs, setRuns] = useState<Run[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [triggering, setTriggering] = useState(false);
  const [dagYAML, setDagYAML] = useState<string | null>(null);

  const [page, setPage] = useState(1);
  const [statusFilter, setStatusFilter] = useState<string | null>("all");
  const [triggerFilter, setTriggerFilter] = useState<string | null>("all");
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
        const dagsRes = await apiFetch("/api/dags");
        const allDags: Dag[] = await dagsRes.json();

        const targetDag = allDags.find((d) => d.ID === id);
        if (!targetDag) {
          setError(`DAG ${id} not found in memory`);
          return;
        }

        setDag(targetDag);

        // Fetch raw YAML source for the Definition tab
        try {
          const yamlRes = await apiFetch(`/api/dags/${id}/yaml`);
          if (yamlRes.ok) setDagYAML(await yamlRes.text());
        } catch {
          // Non-critical — the graph still works without the YAML
        }

        // Build react flow nodes off schema dynamically
        const initialNodes =
          targetDag.Tasks?.map((task) => ({
            id: task.ID,
            data: { label: task.ID },
            position: { x: 0, y: 0 },
            style: {
              background: "var(--mantine-color-dark-6)",
              color: "white",
              border: "1px solid var(--mantine-color-blue-filled)",
              borderRadius: "8px",
              padding: "10px 15px",
              fontSize: "12px",
              fontWeight: 600,
              width: nodeWidth,
              wordBreak: "break-word" as const,
              whiteSpace: "pre-wrap" as const,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              textAlign: "center" as const,
            },
          })) || [];

        const initialEdges: Edge[] = [];
        targetDag.Tasks?.forEach((task) => {
          if (task.DependsOn && task.DependsOn.length > 0) {
            task.DependsOn.forEach((dep) => {
              initialEdges.push({
                id: `e-${dep}-${task.ID}`,
                source: dep,
                target: task.ID,
                animated: true,
                style: { stroke: "var(--mantine-color-blue-filled)" },
                markerEnd: {
                  type: MarkerType.ArrowClosed,
                  width: 20,
                  height: 20,
                  color: "var(--mantine-color-blue-filled)",
                },
              });
            });
          }
        });

        const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(
          initialNodes,
          initialEdges,
          "TB" // top-down direction
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

  // Periodic fetching runs list to sync paginated data
  const fetchRuns = async () => {
    if (!id) return;
    try {
      const url = `/api/runs?page=${page}&limit=${limit}&dag_id=${id}&status=${statusFilter || "all"}&trigger=${triggerFilter || "all"}`;
      const runsRes = await apiFetch(url);
      if (runsRes.ok) {
        const runsData = await runsRes.json();
        setRuns(runsData.data || []);
        setTotalRuns(runsData.total || 0);
      } else {
        setRuns([]);
        setTotalRuns(0);
      }
    } catch (err) {
      console.error("Failed to query runs", err);
    }
  };

  useVisibilityPoll(fetchRuns, 5000, [id, page, statusFilter, triggerFilter]);

  const handleTrigger = async () => {
    if (!id) return;
    setTriggering(true);
    try {
      const res = await apiFetch(`/api/dags/${id}/runs`, { method: "POST" });
      if (res.ok) {
        setPage(1); // Reset to first page so they see it
        notifications.show({
          title: "Pipeline Triggered",
          message: `Successfully enqueued a fresh manual run for ${id}`,
          color: "green",
        });
      } else {
        notifications.show({
          title: "Trigger Failed",
          message: `Failed to enqueue manual run for ${id}.`,
          color: "red",
        });
      }
    } catch (err) {
      console.error(err);
      notifications.show({
        title: "Network Error",
        message: "Could not communicate with the API.",
        color: "red",
      });
    } finally {
      setTriggering(false);
    }
  };

  const handleKillRun = async (e: React.MouseEvent, runID: string) => {
    e.stopPropagation();
    try {
      const res = await apiFetch(`/api/runs/${runID}/kill`, {
        method: "POST",
      });
      if (res.ok) {
        notifications.show({
          title: "Run Terminated",
          message: `Termination signal sent to run ${runID}.`,
          color: "orange",
        });
        fetchRuns();
      }
    } catch (err) {
      console.error("Failed to kill run:", err);
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case "success":
        return "green";
      case "failed":
        return "red";
      case "running":
        return "blue";
      case "queued":
        return "yellow";
      default:
        return "gray";
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
          <Button
            variant="subtle"
            leftSection={<IconArrowLeft size={16} />}
            onClick={() => router.push("/dags")}
            color="gray"
          >
            Back
          </Button>
          <Title order={2}>{id}</Title>
          {dag && (
            <Badge variant="light" color="blue" size="lg">
              {dag.Schedule}
            </Badge>
          )}
        </Group>
        {dag && (
          <Button
            leftSection={<IconPlayerPlay size={16} />}
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
        <Grid gutter="xl">
          <Grid.Col span={{ base: 12, lg: 7 }}>
            <Title order={4} mb="md" c="dimmed">
              Execution History
            </Title>
            <Card padding="0" style={{ overflow: "hidden" }}>
              <Table.ScrollContainer minWidth={600}>
                <Table verticalSpacing="md" horizontalSpacing="md" striped highlightOnHover>
                  <Table.Thead>
                    <Table.Tr>
                      <Table.Th
                        style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}
                      >
                        <Text size="sm" fw={700}>
                          Run ID
                        </Text>
                      </Table.Th>
                      <Table.Th
                        style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}
                      >
                        <Menu shadow="md" width={150}>
                          <Menu.Target>
                            <UnstyledButton>
                              <Group gap={4}>
                                <Text
                                  size="sm"
                                  fw={700}
                                  c={statusFilter !== "all" ? "blue" : undefined}
                                >
                                  Status
                                </Text>
                                <IconFilter
                                  size={14}
                                  color={
                                    statusFilter !== "all"
                                      ? "var(--mantine-color-blue-filled)"
                                      : "var(--mantine-color-gray-5)"
                                  }
                                />
                              </Group>
                            </UnstyledButton>
                          </Menu.Target>
                          <Menu.Dropdown>
                            <Menu.Label>Filter by Status</Menu.Label>
                            {[
                              { value: "all", label: "All Statuses" },
                              { value: "success", label: "Success" },
                              { value: "failed", label: "Failed" },
                              { value: "running", label: "Running" },
                            ].map((opt) => (
                              <Menu.Item
                                key={opt.value}
                                onClick={() => {
                                  setStatusFilter(opt.value);
                                  setPage(1);
                                }}
                                leftSection={
                                  statusFilter === opt.value ? (
                                    <IconCheck size={14} />
                                  ) : (
                                    <div style={{ width: 14 }} />
                                  )
                                }
                              >
                                {opt.label}
                              </Menu.Item>
                            ))}
                          </Menu.Dropdown>
                        </Menu>
                      </Table.Th>
                      <Table.Th
                        style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}
                      >
                        <Text size="sm" fw={700}>
                          Execution Date
                        </Text>
                      </Table.Th>
                      <Table.Th
                        style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}
                      >
                        <Menu shadow="md" width={150}>
                          <Menu.Target>
                            <UnstyledButton>
                              <Group gap={4}>
                                <Text
                                  size="sm"
                                  fw={700}
                                  c={triggerFilter !== "all" ? "blue" : undefined}
                                >
                                  Trigger
                                </Text>
                                <IconFilter
                                  size={14}
                                  color={
                                    triggerFilter !== "all"
                                      ? "var(--mantine-color-blue-filled)"
                                      : "var(--mantine-color-gray-5)"
                                  }
                                />
                              </Group>
                            </UnstyledButton>
                          </Menu.Target>
                          <Menu.Dropdown>
                            <Menu.Label>Filter by Trigger</Menu.Label>
                            {[
                              { value: "all", label: "All Triggers" },
                              { value: "manual", label: "Manual" },
                              { value: "scheduled", label: "Scheduled" },
                              { value: "triggered", label: "Triggered" },
                            ].map((opt) => (
                              <Menu.Item
                                key={opt.value}
                                onClick={() => {
                                  setTriggerFilter(opt.value);
                                  setPage(1);
                                }}
                                leftSection={
                                  triggerFilter === opt.value ? (
                                    <IconCheck size={14} />
                                  ) : (
                                    <div style={{ width: 14 }} />
                                  )
                                }
                              >
                                {opt.label}
                              </Menu.Item>
                            ))}
                          </Menu.Dropdown>
                        </Menu>
                      </Table.Th>
                      <Table.Th
                        style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}
                      >
                        <Text size="sm" fw={700}>
                          Elapsed Time
                        </Text>
                      </Table.Th>
                      <Table.Th
                        style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}
                      >
                        <Text size="sm" fw={700}>
                          Actions
                        </Text>
                      </Table.Th>
                    </Table.Tr>
                  </Table.Thead>
                  <Table.Tbody>
                    {runs?.map((run) => (
                      <Table.Tr
                        key={run.ID}
                        onClick={() => router.push(`/runs/?id=${run.ID}`)}
                        style={{ cursor: "pointer" }}
                      >
                        <Table.Td>
                          <Text size="sm" fw={500}>
                            {run.ID}
                          </Text>
                        </Table.Td>
                        <Table.Td>
                          <Badge
                            color={getStatusColor(run.Status)}
                            variant="light"
                            size="sm"
                            radius="sm"
                          >
                            {run.Status.toUpperCase()}
                          </Badge>
                        </Table.Td>
                        <Table.Td>
                          <Text size="sm">{new Date(run.ExecDate).toLocaleString()}</Text>
                        </Table.Td>
                        <Table.Td>
                          {run.TriggerType === "manual" ? (
                            <Badge
                              variant="light"
                              color="blue"
                              size="sm"
                              leftSection={
                                <IconUser
                                  size={12}
                                  style={{
                                    display: "flex",
                                    alignItems: "center",
                                    marginTop: "2px",
                                  }}
                                />
                              }
                            >
                              Manual
                            </Badge>
                          ) : run.TriggerType === "scheduled" ? (
                            <Badge
                              variant="light"
                              color="teal"
                              size="sm"
                              leftSection={
                                <IconRobot
                                  size={12}
                                  style={{
                                    display: "flex",
                                    alignItems: "center",
                                    marginTop: "2px",
                                  }}
                                />
                              }
                            >
                              Scheduled
                            </Badge>
                          ) : (
                            <Badge
                              variant="light"
                              color="violet"
                              size="sm"
                              leftSection={
                                <IconActivity
                                  size={12}
                                  style={{
                                    display: "flex",
                                    alignItems: "center",
                                    marginTop: "2px",
                                  }}
                                />
                              }
                            >
                              Triggered
                            </Badge>
                          )}
                        </Table.Td>
                        <Table.Td>
                          <Text size="sm" c="dimmed">
                            {run.CompletedAt
                              ? `${Math.max(1, Math.floor((new Date(run.CompletedAt).getTime() - new Date(run.CreatedAt).getTime()) / 1000))}s`
                              : run.Status === "running"
                                ? `${Math.max(1, Math.floor((new Date().getTime() - new Date(run.CreatedAt).getTime()) / 1000))}s`
                                : "-"}
                          </Text>
                        </Table.Td>
                        <Table.Td>
                          {run.Status === "running" && (
                            <Tooltip label="Kill Run">
                              <ActionIcon
                                variant="light"
                                color="red"
                                onClick={(e) => handleKillRun(e, run.ID)}
                                size="sm"
                              >
                                <IconPlayerStop size={14} />
                              </ActionIcon>
                            </Tooltip>
                          )}
                        </Table.Td>
                      </Table.Tr>
                    ))}
                    {(!runs || runs.length === 0) && (
                      <Table.Tr>
                        <Table.Td colSpan={6} align="center" py="xl">
                          <Text c="dimmed">No past runs found for this DAG.</Text>
                        </Table.Td>
                      </Table.Tr>
                    )}
                  </Table.Tbody>
                </Table>
              </Table.ScrollContainer>
              {totalRuns > limit && (
                <Group
                  justify="center"
                  p="md"
                  style={{ borderTop: "1px solid var(--mantine-color-default-border)" }}
                >
                  <Pagination
                    total={Math.ceil(totalRuns / limit)}
                    value={page}
                    onChange={setPage}
                    withEdges
                  />
                </Group>
              )}
            </Card>
          </Grid.Col>
          <Grid.Col span={{ base: 12, lg: 5 }}>
            <Tabs defaultValue="graph" variant="outline">
              <Tabs.List mb="md">
                <Tabs.Tab value="graph">Pipeline Graph</Tabs.Tab>
                <Tabs.Tab value="definition">Definition</Tabs.Tab>
              </Tabs.List>
              <Tabs.Panel value="graph">
                <Card style={{ height: "600px" }} p="0">
                  <ReactFlow
                    nodes={nodes}
                    edges={edges}
                    onNodesChange={onNodesChange}
                    onEdgesChange={onEdgesChange}
                    fitView
                    attributionPosition="bottom-right"
                  >
                    <Background color="var(--mantine-color-dark-4)" gap={16} />
                    <Controls />
                  </ReactFlow>
                </Card>
              </Tabs.Panel>
              <Tabs.Panel value="definition">
                <Card p="0" style={{ height: "600px", overflow: "hidden" }}>
                  <ScrollArea h="600px">
                    {dagYAML ? (
                      <SyntaxHighlighter
                        language="yaml"
                        style={colorScheme === "dark" ? atomOneDark : atomOneLight}
                        customStyle={{
                          margin: 0,
                          padding: "16px",
                          background: "transparent",
                          fontSize: "12px",
                          lineHeight: 1.7,
                        }}
                      >
                        {dagYAML}
                      </SyntaxHighlighter>
                    ) : (
                      <Text c="dimmed" size="sm" p="md">
                        YAML source not available.
                      </Text>
                    )}
                  </ScrollArea>
                </Card>
              </Tabs.Panel>
            </Tabs>
          </Grid.Col>
        </Grid>
      )}
    </>
  );
}

// ---------------------------------------------------------------------------
// Root export: switch between list and detail based on ?id param
// ---------------------------------------------------------------------------
function DagsPageContent() {
  const searchParams = useSearchParams();
  const id = searchParams.get("id");
  return id ? <DagDetailsContent /> : <DagListContent />;
}

export default function DagDetails() {
  return (
    <Suspense fallback={<Skeleton height={400} />}>
      <DagsPageContent />
    </Suspense>
  );
}
