import {
  Table,
  Group,
  Text,
  Badge,
  ActionIcon,
  Tooltip,
  Pagination,
  Card,
  Center,
} from "@mantine/core";
import { IconPlayerStop } from "@tabler/icons-react";
import { useRouter } from "next/navigation";
import { StatusBadge } from "../ui/StatusBadge";
import { TriggerBadge } from "../ui/TriggerBadge";
import { TableColumnFilter } from "../ui/TableColumnFilter";
import { EmptyState } from "../ui/EmptyState";
import { apiFetch } from "../../lib/apiFetch";
import { notifications } from "@mantine/notifications";

export interface Run {
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
}

interface RunsTableProps {
  runs: Run[];
  loading?: boolean;
  dags?: Dag[]; // Empty if DAG filtering should be disabled/hidden

  totalRuns: number;
  limit: number;
  page: number;
  onPageChange: (p: number) => void;

  dagFilter?: string | null;
  onDagFilterChange?: (val: string) => void;
  statusFilter: string | null;
  onStatusFilterChange: (val: string) => void;
  triggerFilter: string | null;
  onTriggerFilterChange: (val: string) => void;

  onRunKilled?: () => void;
}

export function RunsTable({
  runs,
  loading,
  dags,
  totalRuns,
  limit,
  page,
  onPageChange,
  dagFilter,
  onDagFilterChange,
  statusFilter,
  onStatusFilterChange,
  triggerFilter,
  onTriggerFilterChange,
  onRunKilled,
}: RunsTableProps) {
  const router = useRouter();
  const showDagColumn = dags !== undefined && onDagFilterChange !== undefined;

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
        if (onRunKilled) onRunKilled();
      }
    } catch (err) {
      console.error("Failed to kill run:", err);
    }
  };

  return (
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
              {showDagColumn && (
                <Table.Th style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}>
                  <TableColumnFilter
                    label="DAG"
                    menuLabel="Filter by DAG"
                    value={dagFilter || "all"}
                    onChange={(val) => {
                      onDagFilterChange(val);
                      onPageChange(1);
                    }}
                    options={[
                      { value: "all", label: "All DAGs" },
                      ...dags.map((d) => ({ value: d.ID, label: d.ID })),
                    ]}
                    width={200}
                  />
                </Table.Th>
              )}
              <Table.Th style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}>
                <TableColumnFilter
                  label="Status"
                  menuLabel="Filter by Status"
                  value={statusFilter || "all"}
                  onChange={(val) => {
                    onStatusFilterChange(val);
                    onPageChange(1);
                  }}
                  options={[
                    { value: "all", label: "All Statuses" },
                    { value: "success", label: "Success" },
                    { value: "failed", label: "Failed" },
                    { value: "cancelled", label: "Cancelled" },
                    { value: "running", label: "Running" },
                  ]}
                />
              </Table.Th>
              <Table.Th style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}>
                <Text size="sm" fw={700}>
                  Execution Date
                </Text>
              </Table.Th>
              <Table.Th style={{ borderBottom: "2px solid var(--border-color)", height: "45px" }}>
                <TableColumnFilter
                  label="Trigger"
                  menuLabel="Filter by Trigger"
                  value={triggerFilter || "all"}
                  onChange={(val) => {
                    onTriggerFilterChange(val);
                    onPageChange(1);
                  }}
                  options={[
                    { value: "all", label: "All Triggers" },
                    { value: "manual", label: "Manual" },
                    { value: "scheduled", label: "Scheduled" },
                    { value: "triggered", label: "Triggered" },
                  ]}
                />
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
                {showDagColumn && (
                  <Table.Td>
                    <Badge variant="outline" color="gray">
                      {run.DAGID}
                    </Badge>
                  </Table.Td>
                )}
                <Table.Td>
                  <StatusBadge status={run.Status} />
                </Table.Td>
                <Table.Td>
                  <Text size="sm">{new Date(run.ExecDate).toLocaleString()}</Text>
                </Table.Td>
                <Table.Td>
                  <TriggerBadge trigger={run.TriggerType} />
                </Table.Td>
                <Table.Td>
                  <Text size="sm" c="dimmed">
                    {run.CompletedAt
                      ? `${Math.max(1, Math.floor((new Date(run.CompletedAt).getTime() - new Date(run.CreatedAt).getTime()) / 1000))}s`
                      : run.Status === "running"
                        ? `${Math.max(1, Math.floor((new Date().getTime() - new Date(run.CreatedAt).getTime()) / 1000))}s`
                        : "—"}
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
          </Table.Tbody>
        </Table>

        {(!runs || runs.length === 0) && !loading && (
          <EmptyState
            title="No runs found"
            description="Adjust your filters or trigger a new pipeline run."
          />
        )}
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
            onChange={onPageChange}
            color="cyan"
            withEdges
          />
        </Group>
      )}
    </Card>
  );
}
