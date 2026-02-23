"use client";

import { useEffect, useState } from "react";
import { useVisibilityPoll } from "../lib/useVisibilityPoll";
import {
  Title,
  Card,
  Table,
  Badge,
  Text,
  Group,
  Button,
  Skeleton,
  Select,
  Pagination,
  ActionIcon,
  Tooltip,
  Menu,
  UnstyledButton,
  Alert,
  List,
  Switch,
} from "@mantine/core";
import {
  IconRefresh,
  IconX,
  IconActivity,
  IconAlertCircle,
  IconRobot,
  IconUser,
  IconPlayerPlay,
  IconTimelineEvent,
  IconFilter,
  IconCheck,
  IconPlayerStop,
  IconSitemap,
} from "@tabler/icons-react";
import { useRouter } from "next/navigation";
import { notifications } from "@mantine/notifications";
import { apiFetch } from "../lib/apiFetch";

interface Run {
  ID: string;
  DAGID: string;
  Status: string;
  ExecDate: string;
  TriggerType: string;
  CreatedAt: string;
  CompletedAt?: string;
}

interface Dag {
  ID: string;
  Schedule: string;
  Description: string;
  Paused: boolean;
}

interface SystemStats {
  active_runs: number;
  failed_runs_24h: number;
  total_runs: number;
  loaded_dags: number;
}

export default function Dashboard() {
  const [runs, setRuns] = useState<Run[]>([]);
  const [dags, setDags] = useState<Dag[]>([]);
  const [dagErrors, setDagErrors] = useState<Record<string, string>>({});
  const [stats, setStats] = useState<SystemStats | null>(null);
  const [triggering, setTriggering] = useState<Record<string, boolean>>({});
  const [pausing, setPausing] = useState<Record<string, boolean>>({});
  const [loading, setLoading] = useState(true);

  const [page, setPage] = useState(1);
  const [dagFilter, setDagFilter] = useState<string | null>("all");
  const [statusFilter, setStatusFilter] = useState<string | null>("all");
  const [triggerFilter, setTriggerFilter] = useState<string | null>("all");
  const [totalRuns, setTotalRuns] = useState(0);
  const limit = 10;

  const router = useRouter();

  const fetchData = async () => {
    try {
      setLoading(true);
      const [runsRes, dagsRes, errorsRes, statsRes] = await Promise.all([
        apiFetch(
          `/api/runs?page=${page}&limit=${limit}&dag_id=${dagFilter || "all"}&status=${statusFilter || "all"}&trigger=${triggerFilter || "all"}`
        ),
        apiFetch("/api/dags"),
        apiFetch("/api/dags/errors"),
        apiFetch("/api/stats"),
      ]);

      if (runsRes.ok) {
        const runsData = await runsRes.json();
        setRuns(runsData.data || []);
        setTotalRuns(runsData.total || 0);
      }

      if (dagsRes.ok) setDags(await dagsRes.json());
      if (errorsRes.ok) setDagErrors((await errorsRes.json()) || {});
      if (statsRes.ok) setStats(await statsRes.json());
    } catch (err) {
      console.error("Failed to fetch data", err);
    } finally {
      setLoading(false);
    }
  };

  useVisibilityPoll(fetchData, 5000, [page, dagFilter, statusFilter, triggerFilter]);

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
        fetchData();
      }
    } catch (err) {
      console.error(err);
    } finally {
      setTriggering((prev) => ({ ...prev, [dagID]: false }));
    }
  };

  const handleTogglePause = async (dagID: string, currentlyPaused: boolean) => {
    setPausing((prev) => ({ ...prev, [dagID]: true }));
    setDags((prev) => prev.map((d) => (d.ID === dagID ? { ...d, Paused: !currentlyPaused } : d)));
    try {
      const action = currentlyPaused ? "activate" : "pause";
      const res = await apiFetch(`/api/dags/${dagID}/${action}`, { method: "POST" });
      if (!res.ok) {
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
      setDags((prev) => prev.map((d) => (d.ID === dagID ? { ...d, Paused: currentlyPaused } : d)));
    } finally {
      setPausing((prev) => ({ ...prev, [dagID]: false }));
    }
  };

  const handleKillRun = async (e: React.MouseEvent, runID: string) => {
    e.stopPropagation();
    try {
      const res = await apiFetch(`/api/runs/${runID}/kill`, { method: "POST" });
      if (res.ok) {
        notifications.show({
          title: "Run Terminated",
          message: `Termination signal sent to run ${runID}.`,
          color: "orange",
        });
        fetchData();
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
      case "cancelled":
        return "gray";
      default:
        return "gray";
    }
  };

  return (
    <>
      <Group justify="space-between" mb="xl">
        <Title order={2}>Dashboard</Title>
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

      {stats && (
        <Card padding="md" mb="xl">
          <Group grow>
            <div>
              <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                Active Runs
              </Text>
              <Group gap="xs" mt={4}>
                <IconActivity size={18} color="var(--mantine-color-blue-filled)" />
                <Text fw={700} size="xl">
                  {stats.active_runs}
                </Text>
              </Group>
            </div>
            <div>
              <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                Failed Runs (24h)
              </Text>
              <Group gap="xs" mt={4}>
                <IconX
                  size={18}
                  color={
                    stats.failed_runs_24h > 0
                      ? "var(--mantine-color-red-filled)"
                      : "var(--mantine-color-gray-5)"
                  }
                />
                <Text fw={700} size="xl" c={stats.failed_runs_24h > 0 ? "red" : "inherit"}>
                  {stats.failed_runs_24h}
                </Text>
              </Group>
            </div>
            <div>
              <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                Total Operations
              </Text>
              <Group gap="xs" mt={4}>
                <IconTimelineEvent size={18} color="var(--mantine-color-teal-filled)" />
                <Text fw={700} size="xl">
                  {stats.total_runs}
                </Text>
              </Group>
            </div>
            <div>
              <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                Loaded DAGs
              </Text>
              <Group gap="xs" mt={4}>
                <IconSitemap size={18} color="var(--mantine-color-violet-filled)" />
                <Text fw={700} size="xl">
                  {stats.loaded_dags}
                </Text>
              </Group>
            </div>
          </Group>
        </Card>
      )}

      <Title order={4} mb="md" c="dimmed">
        Loaded Workflows
      </Title>
      {loading && dags.length === 0 ? (
        <Skeleton height={200} mb="xl" radius="md" />
      ) : (
        <Card padding="0" mb="xl" style={{ overflow: "hidden" }}>
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
                          maxWidth: "400px",
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
                    <Table.Td colSpan={3}>
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

      <Title order={4} mb="md" c="dimmed">
        Recent Runs
      </Title>
      <Card padding="0">
        <Table.ScrollContainer minWidth={800}>
          <Table verticalSpacing="md" horizontalSpacing="md" striped highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}>
                  <Text size="sm" fw={700}>
                    Run ID
                  </Text>
                </Table.Th>
                <Table.Th style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}>
                  <Menu shadow="md" width={200}>
                    <Menu.Target>
                      <UnstyledButton>
                        <Group gap={4}>
                          <Text size="sm" fw={700} c={dagFilter !== "all" ? "blue" : undefined}>
                            Dag ID
                          </Text>
                          <IconFilter
                            size={14}
                            color={
                              dagFilter !== "all"
                                ? "var(--mantine-color-blue-filled)"
                                : "var(--mantine-color-gray-5)"
                            }
                          />
                        </Group>
                      </UnstyledButton>
                    </Menu.Target>
                    <Menu.Dropdown>
                      <Menu.Label>Filter by DAG</Menu.Label>
                      <Menu.Item
                        onClick={() => {
                          setDagFilter("all");
                          setPage(1);
                        }}
                        leftSection={
                          dagFilter === "all" ? (
                            <IconCheck size={14} />
                          ) : (
                            <div style={{ width: 14 }} />
                          )
                        }
                      >
                        All DAGs
                      </Menu.Item>
                      {dags.map((d) => (
                        <Menu.Item
                          key={d.ID}
                          onClick={() => {
                            setDagFilter(d.ID);
                            setPage(1);
                          }}
                          leftSection={
                            dagFilter === d.ID ? (
                              <IconCheck size={14} />
                            ) : (
                              <div style={{ width: 14 }} />
                            )
                          }
                        >
                          {d.ID}
                        </Menu.Item>
                      ))}
                    </Menu.Dropdown>
                  </Menu>
                </Table.Th>
                <Table.Th style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}>
                  <Menu shadow="md" width={150}>
                    <Menu.Target>
                      <UnstyledButton>
                        <Group gap={4}>
                          <Text size="sm" fw={700} c={statusFilter !== "all" ? "blue" : undefined}>
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
                        { value: "cancelled", label: "Cancelled" },
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
                <Table.Th style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}>
                  <Text size="sm" fw={700}>
                    Execution Date
                  </Text>
                </Table.Th>
                <Table.Th style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}>
                  <Menu shadow="md" width={150}>
                    <Menu.Target>
                      <UnstyledButton>
                        <Group gap={4}>
                          <Text size="sm" fw={700} c={triggerFilter !== "all" ? "blue" : undefined}>
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
                <Table.Th style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}>
                  <Text size="sm" fw={700}>
                    Elapsed Time
                  </Text>
                </Table.Th>
                <Table.Th style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}>
                  <Text size="sm" fw={700}>
                    Actions
                  </Text>
                </Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {runs.map((run) => (
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
                    <Badge variant="outline" color="gray">
                      {run.DAGID}
                    </Badge>
                  </Table.Td>
                  <Table.Td>
                    <Badge color={getStatusColor(run.Status)} variant="light" size="sm" radius="sm">
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
                            style={{ display: "flex", alignItems: "center", marginTop: "2px" }}
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
                            style={{ display: "flex", alignItems: "center", marginTop: "2px" }}
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
                            style={{ display: "flex", alignItems: "center", marginTop: "2px" }}
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
              {(!runs || runs.length === 0) && !loading && (
                <Table.Tr>
                  <Table.Td colSpan={7} align="center" py="xl">
                    <Text c="dimmed">No runs found for this configuration.</Text>
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
              color="cyan"
              withEdges
            />
          </Group>
        )}
      </Card>
    </>
  );
}
