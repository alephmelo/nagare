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
  ActionIcon,
  Tooltip,
  Alert,
  List,
  Switch,
} from "@mantine/core";
import {
  IconRefresh,
  IconX,
  IconActivity,
  IconAlertCircle,
  IconPlayerPlay,
  IconTimelineEvent,
  IconSitemap,
} from "@tabler/icons-react";
import { useRouter } from "next/navigation";
import { notifications } from "@mantine/notifications";
import { apiFetch } from "../lib/apiFetch";
import { PageHeader } from "../components/ui/PageHeader";
import { RunsTable, Run } from "../components/blocks/RunsTable";

// Run interface imported from RunsTable

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

  // Table actions and statuses moved to RunsTable

  return (
    <>
      <PageHeader
        title="Dashboard"
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

      <RunsTable
        runs={runs}
        loading={loading}
        dags={dags}
        totalRuns={totalRuns}
        limit={limit}
        page={page}
        onPageChange={setPage}
        dagFilter={dagFilter}
        onDagFilterChange={setDagFilter}
        statusFilter={statusFilter}
        onStatusFilterChange={setStatusFilter}
        triggerFilter={triggerFilter}
        onTriggerFilterChange={setTriggerFilter}
        onRunKilled={fetchData}
      />
    </>
  );
}
