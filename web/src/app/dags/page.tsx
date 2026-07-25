"use client";

import { Suspense, useCallback, useEffect, useMemo, useRef, useState } from "react";
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
  Alert,
  ActionIcon,
  Tooltip,
  List,
  Tabs,
  ScrollArea,
  Switch,
  useMantineColorScheme,
} from "@mantine/core";
import { IconAlertCircle, IconPlayerPlay, IconRefresh } from "@tabler/icons-react";
import { useRouter } from "next/navigation";
import { PageHeader } from "../../components/ui/PageHeader";
import { RunsTable, Run } from "../../components/blocks/RunsTable";
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
  Handle,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { notifications } from "@mantine/notifications";
import { layoutTopologyStages } from "../../components/executionTopologyLayout";
import { projectExecutionTopology } from "../../lib/executionTopology";
import SyntaxHighlighter from "react-syntax-highlighter";
import { atomOneDark, atomOneLight } from "react-syntax-highlighter/dist/esm/styles/hljs";

// Types
interface TaskDef {
  ID: string;
  Type: string;
  Command: string;
  DependsOn: string[];
  MapOver?: string;
}

interface Dag {
  ID: string;
  Schedule: string;
  Description: string;
  Tasks: TaskDef[];
  Paused: boolean;
}

// Run interface imported from RunsTable

const nodeWidth = 280;
const nodeHeight = 60;

// Custom node with tooltip
function DagNodeComponent({
  data,
}: {
  data: { label: string; taskType?: string; command?: string; status?: string };
}) {
  return (
    <Tooltip
      label={
        <div>
          <Text size="xs" fw={600}>
            {data.label}
          </Text>
          {data.taskType && (
            <Text size="xs" c="dimmed">
              Type: {data.taskType}
            </Text>
          )}
          {data.command && (
            <Text size="xs" c="dimmed" lineClamp={2}>
              Cmd: {data.command}
            </Text>
          )}
          {data.status && (
            <Text size="xs" c="dimmed">
              Status: {data.status}
            </Text>
          )}
        </div>
      }
      multiline
      w={250}
      position="top"
      openDelay={300}
    >
      <div style={{ width: "100%", textAlign: "center", padding: "10px 15px" }}>
        <Handle
          type="target"
          position={Position.Top}
          style={{ visibility: "hidden", pointerEvents: "none" }}
        />
        {data.label}
        <Handle
          type="source"
          position={Position.Bottom}
          style={{ visibility: "hidden", pointerEvents: "none" }}
        />
      </div>
    </Tooltip>
  );
}

const nodeTypes = { dagNode: DagNodeComponent };

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
        const run = await res.json();
        router.push(`/runs/?id=${run.ID}`);
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
      <PageHeader
        title="DAGs"
        actions={
          <Button leftSection={<IconRefresh size={16} />} variant="light" onClick={fetchData}>
            Refresh
          </Button>
        }
      />

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
                  <Table.Th
                    style={{ borderBottom: "2px solid var(--mantine-color-default-border)" }}
                  >
                    <Text size="sm" fw={700}>
                      Pipeline
                    </Text>
                  </Table.Th>
                  <Table.Th
                    style={{ borderBottom: "2px solid var(--mantine-color-default-border)" }}
                  >
                    <Text size="sm" fw={700}>
                      Schedule
                    </Text>
                  </Table.Th>
                  <Table.Th
                    style={{ borderBottom: "2px solid var(--mantine-color-default-border)" }}
                  >
                    <Text size="sm" fw={700}>
                      Tasks
                    </Text>
                  </Table.Th>
                  <Table.Th
                    style={{
                      borderBottom: "2px solid var(--mantine-color-default-border)",
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
                    className="row-hover"
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
                        <Badge variant="light" color="yellow" size="sm" radius="xl">
                          Paused
                        </Badge>
                      ) : (
                        <Badge variant="light" color="blue" size="sm" radius="xl">
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
  const [runtimeTasks, setRuntimeTasks] = useState<
    { ID: string; TaskID: string; Status: string }[]
  >([]);
  const activeDagIDRef = useRef<string | null>(id);
  const runsRequestGenerationRef = useRef(0);

  const [page, setPage] = useState(1);
  const [statusFilter, setStatusFilter] = useState<string | null>("all");
  const [triggerFilter, setTriggerFilter] = useState<string | null>("all");
  const [totalRuns, setTotalRuns] = useState(0);
  const limit = 10;

  // React Flow strict local states
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const topology = useMemo(
    () => projectExecutionTopology(dag?.Tasks ?? [], runtimeTasks),
    [dag, runtimeTasks]
  );

  useEffect(() => {
    const definitionByID = new Map(dag?.Tasks.map((task) => [task.ID, task]) ?? []);
    const projectedNodes: Node[] = topology.nodes.map((node) => {
      const task = node.definitionId ? definitionByID.get(node.definitionId) : undefined;
      const status = node.status;
      let borderColor = "var(--mantine-color-blue-filled)";
      let background = "var(--node-bg)";
      let animation = "none";
      if (status === "success") {
        borderColor = "var(--mantine-color-green-filled)";
        background = "rgba(43, 138, 62, 0.1)";
      } else if (status === "failed") {
        borderColor = "var(--mantine-color-red-filled)";
        background = "rgba(224, 49, 49, 0.08)";
      } else if (status === "running") animation = "pulseBlue 2s infinite";
      else if (status === "queued" || status === "pending")
        borderColor = "var(--mantine-color-gray-6)";
      return {
        id: node.id,
        type: "dagNode",
        data: { label: node.id, taskType: task?.Type, command: task?.Command, status },
        position: { x: 0, y: 0 },
        style: {
          background,
          color: "var(--node-text)",
          border: `1px solid ${borderColor}`,
          borderRadius: "8px",
          fontSize: "12px",
          fontWeight: 600,
          fontFamily: "var(--font-outfit)",
          width: nodeWidth,
          wordBreak: "break-word" as const,
          whiteSpace: "pre-wrap" as const,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          textAlign: "center" as const,
          animation,
          transition: "all 0.3s ease",
        },
      };
    });
    const statusByID = new Map(topology.nodes.map((node) => [node.id, node.status]));
    const projectedEdges: Edge[] = topology.edges.map((edge) => {
      const status = statusByID.get(edge.source);
      const color =
        status === "success"
          ? "var(--mantine-color-green-filled)"
          : status === "failed"
            ? "var(--mantine-color-red-filled)"
            : status
              ? "var(--mantine-color-gray-6)"
              : "var(--mantine-color-blue-filled)";
      return {
        ...edge,
        animated: runs.some((run) => run.Status === "running"),
        style: { stroke: color, transition: "stroke 0.3s ease" },
        markerEnd: { type: MarkerType.ArrowClosed, width: 20, height: 20, color },
      };
    });
    setNodes(layoutTopologyStages(projectedNodes, topology.stages, nodeWidth, nodeHeight));
    setEdges(projectedEdges);
  }, [dag, topology, runs, setNodes, setEdges]);

  // Responsive graph height based on task count
  const graphHeight = Math.max(400, Math.min(800, (dag?.Tasks?.length || 5) * 100));

  useEffect(() => {
    if (!id) return;
    activeDagIDRef.current = id;
    runsRequestGenerationRef.current += 1;
    setDag(null);
    setRuntimeTasks([]);
    setDagYAML(null);

    // Initial fetch of DAG specific definitions
    const initializeView = async () => {
      setLoading(true);
      try {
        const dagsRes = await apiFetch("/api/dags");
        const allDags: Dag[] = await dagsRes.json();
        if (activeDagIDRef.current !== id) return;

        const targetDag = allDags.find((d) => d.ID === id);
        if (!targetDag) {
          setError(`DAG ${id} not found in memory`);
          return;
        }

        setDag(targetDag);

        // Fetch raw YAML source for the Definition tab
        try {
          const yamlRes = await apiFetch(`/api/dags/${id}/yaml`);
          if (yamlRes.ok) {
            const yaml = await yamlRes.text();
            if (activeDagIDRef.current === id) setDagYAML(yaml);
          }
        } catch {
          // Non-critical — the graph still works without the YAML
        }

      } catch (err) {
        console.error(err);
        if (activeDagIDRef.current === id)
          setError("Failed to load DAG schema from database.");
      } finally {
        if (activeDagIDRef.current === id) setLoading(false);
      }
    };

    initializeView();
  }, [id]);

  // Periodic fetching runs list to sync paginated data
  const fetchRuns = useCallback(async () => {
    if (!id) return;
    const requestedID = id;
    const requestGeneration = ++runsRequestGenerationRef.current;
    const isCurrentRequest = () =>
      activeDagIDRef.current === requestedID &&
      runsRequestGenerationRef.current === requestGeneration;
    try {
      const url = `/api/runs?page=${page}&limit=${limit}&dag_id=${id}&status=${statusFilter || "all"}&trigger=${triggerFilter || "all"}`;
      const runsRes = await apiFetch(url);
      if (!isCurrentRequest()) return;
      if (runsRes.ok) {
        const runsData = await runsRes.json();
        if (!isCurrentRequest()) return;
        setRuns(runsData.data || []);
        setTotalRuns(runsData.total || 0);

        // --- Live Physics-Based Visualizer Sync ---
        const rData = runsData.data || [];
        let targetRun = rData.find((r: Run) => r.Status === "running");
        if (!targetRun && rData.length > 0) {
          targetRun = rData[0]; // Fall back to most recent run if nothing is running
        }

        if (targetRun) {
          const tasksRes = await apiFetch(`/api/runs/${targetRun.ID}/tasks`);
          if (!isCurrentRequest()) return;
          if (tasksRes.ok) {
            const tasksData = await tasksRes.json();
            if (isCurrentRequest()) setRuntimeTasks(tasksData);
          } else if (isCurrentRequest()) setRuntimeTasks([]);
        } else if (isCurrentRequest()) setRuntimeTasks([]);
      } else {
        if (isCurrentRequest()) {
          setRuns([]);
          setTotalRuns(0);
          setRuntimeTasks([]);
        }
      }
    } catch (err) {
      console.error("Failed to query runs", err);
      if (isCurrentRequest()) setRuntimeTasks([]);
    }
  }, [id, page, statusFilter, triggerFilter]);

  useVisibilityPoll(fetchRuns, 5000, [fetchRuns]);

  const handleTrigger = async () => {
    if (!id) return;
    setTriggering(true);
    try {
      const res = await apiFetch(`/api/dags/${id}/runs`, { method: "POST" });
      if (res.ok) {
        const run = await res.json();
        router.push(`/runs/?id=${run.ID}`);
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

  if (!id) {
    return (
      <Alert color="red" title="Error">
        No DAG ID provided in URL parameters.
      </Alert>
    );
  }

  return (
    <>
      <PageHeader
        title={id as string}
        showBack
        backTo="/dags"
        badge={
          <Group gap="xs">
            {dag && (
              <Badge variant="light" color="blue" size="lg">
                {dag.Schedule}
              </Badge>
            )}
            {runs.some((r) => r.Status === "running") && (
              <Badge
                variant="filled"
                color="red"
                size="lg"
                style={{
                  animation: "pulseRedAlert 2s infinite",
                  fontWeight: 800,
                  letterSpacing: "1px",
                }}
              >
                LIVE TELEMETRY
              </Badge>
            )}
          </Group>
        }
        subtitle={dag?.Description}
        actions={
          dag ? (
            <Button
              leftSection={<IconPlayerPlay size={16} />}
              onClick={handleTrigger}
              loading={triggering}
            >
              Trigger Pipeline
            </Button>
          ) : undefined
        }
      />

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
            <RunsTable
              runs={runs}
              loading={loading}
              totalRuns={totalRuns}
              limit={limit}
              page={page}
              onPageChange={setPage}
              statusFilter={statusFilter}
              onStatusFilterChange={setStatusFilter}
              triggerFilter={triggerFilter}
              onTriggerFilterChange={setTriggerFilter}
              onRunKilled={fetchRuns}
            />
          </Grid.Col>
          <Grid.Col span={{ base: 12, lg: 5 }}>
            <Tabs defaultValue="graph" variant="outline">
              <Tabs.List mb="md">
                <Tabs.Tab value="graph">Pipeline Graph</Tabs.Tab>
                <Tabs.Tab value="definition">Definition</Tabs.Tab>
              </Tabs.List>
              <Tabs.Panel value="graph">
                <Card style={{ height: `${graphHeight}px`, position: "relative" }} p="0">
                  <ReactFlow
                    nodes={nodes}
                    edges={edges}
                    onNodesChange={onNodesChange}
                    onEdgesChange={onEdgesChange}
                    nodeTypes={nodeTypes}
                    fitView
                    attributionPosition="bottom-right"
                  >
                    <Background color="var(--graph-grid)" gap={16} />
                    <Controls />
                  </ReactFlow>
                  {/* Graph Legend */}
                  <Group
                    gap={4}
                    style={{
                      position: "absolute",
                      top: 6,
                      left: 6,
                      zIndex: 5,
                      backgroundColor: "var(--panel-bg)",
                      border: "1px solid var(--mantine-color-default-border)",
                      borderRadius: "var(--mantine-radius-sm)",
                      padding: "3px 8px",
                      opacity: 0.85,
                    }}
                  >
                    <Text size="10px" fw={600} c="dimmed">
                      Legend
                    </Text>
                    <Badge size="xs" color="blue" variant="light" style={{ fontSize: 9 }}>
                      Default
                    </Badge>
                    <Badge size="xs" color="green" variant="light" style={{ fontSize: 9 }}>
                      Success
                    </Badge>
                    <Badge size="xs" color="red" variant="light" style={{ fontSize: 9 }}>
                      Failed
                    </Badge>
                    <Badge size="xs" color="blue" variant="filled" style={{ fontSize: 9 }}>
                      Running
                    </Badge>
                    <Badge size="xs" color="gray" variant="light" style={{ fontSize: 9 }}>
                      Pending
                    </Badge>
                  </Group>
                </Card>
              </Tabs.Panel>
              <Tabs.Panel value="definition">
                <Card p="0" style={{ height: `${graphHeight}px`, overflow: "hidden" }}>
                  <ScrollArea h={`${graphHeight}px`}>
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
