"use client";

import { useEffect, useState } from "react";
import {
  Title,
  Card,
  Table,
  Badge,
  Text,
  Group,
  Button,
  Skeleton,
  Stack,
  ThemeIcon,
  SimpleGrid,
  Tooltip,
  Alert,
} from "@mantine/core";
import {
  IconRefresh,
  IconServer,
  IconCircleCheck,
  IconCircleX,
  IconClockHour4,
  IconInfoCircle,
} from "@tabler/icons-react";
import { apiFetch } from "../../lib/apiFetch";

interface WorkerInfo {
  worker_id: string;
  pools: string[];
  hostname: string;
  max_tasks: number;
  last_seen: string;
  status: string;
}

function timeSince(dateStr: string): string {
  const diff = Math.floor((Date.now() - new Date(dateStr).getTime()) / 1000);
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  return `${Math.floor(diff / 3600)}h ago`;
}

export default function WorkersPage() {
  const [workers, setWorkers] = useState<WorkerInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [notAvailable, setNotAvailable] = useState(false);

  const fetchWorkers = async () => {
    try {
      setLoading(true);
      const res = await apiFetch("/api/workers");
      if (res.status === 404) {
        setNotAvailable(true);
        return;
      }
      if (res.ok) {
        const data = await res.json();
        setWorkers(data || []);
        setNotAvailable(false);
      }
    } catch (err) {
      console.error("Failed to fetch workers", err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchWorkers();
    const interval = setInterval(fetchWorkers, 5000);
    return () => clearInterval(interval);
  }, []);

  const online = workers.filter((w) => w.status === "online");
  const offline = workers.filter((w) => w.status === "offline");
  const totalCapacity = online.reduce((sum, w) => sum + w.max_tasks, 0);
  const allPools = Array.from(new Set(workers.flatMap((w) => w.pools)));

  return (
    <>
      <Group justify="space-between" mb="xl">
        <Title order={2}>Workers</Title>
        <Button
          leftSection={<IconRefresh size={16} />}
          variant="light"
          onClick={fetchWorkers}
        >
          Refresh
        </Button>
      </Group>

      {notAvailable ? (
        <Alert
          variant="light"
          color="blue"
          icon={<IconInfoCircle />}
          title="Cluster mode not active"
        >
          The workers API is only available when nagare is running with a
          coordinator attached (i.e. remote workers have joined the cluster).
          Start nagare with remote workers to see cluster status here.
        </Alert>
      ) : loading && workers.length === 0 ? (
        <Stack gap="md">
          <Skeleton height={80} radius="md" />
          <Skeleton height={200} radius="md" />
        </Stack>
      ) : (
        <>
          <SimpleGrid cols={{ base: 2, sm: 4 }} mb="xl">
            <Card padding="md">
              <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                Total Workers
              </Text>
              <Group gap="xs" mt={4}>
                <ThemeIcon variant="light" color="blue" size="sm">
                  <IconServer size={14} />
                </ThemeIcon>
                <Text fw={700} size="xl">
                  {workers.length}
                </Text>
              </Group>
            </Card>
            <Card padding="md">
              <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                Online
              </Text>
              <Group gap="xs" mt={4}>
                <ThemeIcon variant="light" color="green" size="sm">
                  <IconCircleCheck size={14} />
                </ThemeIcon>
                <Text fw={700} size="xl" c="green">
                  {online.length}
                </Text>
              </Group>
            </Card>
            <Card padding="md">
              <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                Offline
              </Text>
              <Group gap="xs" mt={4}>
                <ThemeIcon variant="light" color="red" size="sm">
                  <IconCircleX size={14} />
                </ThemeIcon>
                <Text fw={700} size="xl" c={offline.length > 0 ? "red" : "inherit"}>
                  {offline.length}
                </Text>
              </Group>
            </Card>
            <Card padding="md">
              <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                Total Capacity
              </Text>
              <Group gap="xs" mt={4}>
                <ThemeIcon variant="light" color="teal" size="sm">
                  <IconServer size={14} />
                </ThemeIcon>
                <Text fw={700} size="xl">
                  {totalCapacity}
                </Text>
              </Group>
            </Card>
          </SimpleGrid>

          {allPools.length > 0 && (
            <>
              <Title order={4} mb="md" c="dimmed">
                Pools
              </Title>
              <Card padding="md" mb="xl">
                <Group gap="xs">
                  {allPools.map((pool) => {
                    const poolWorkers = workers.filter((w) =>
                      w.pools.includes(pool)
                    );
                    const poolOnline = poolWorkers.filter(
                      (w) => w.status === "online"
                    );
                    const poolCapacity = poolOnline.reduce(
                      (sum, w) => sum + w.max_tasks,
                      0
                    );
                    return (
                      <Tooltip
                        key={pool}
                        label={`${poolOnline.length}/${poolWorkers.length} workers online · ${poolCapacity} max tasks`}
                        position="bottom"
                      >
                        <Badge
                          variant="light"
                          color={poolOnline.length > 0 ? "teal" : "gray"}
                          size="lg"
                          radius="sm"
                        >
                          {pool} · {poolOnline.length}/{poolWorkers.length}
                        </Badge>
                      </Tooltip>
                    );
                  })}
                </Group>
              </Card>
            </>
          )}

          <Title order={4} mb="md" c="dimmed">
            Worker Registry
          </Title>
          <Card padding="0">
            <Table.ScrollContainer minWidth={600}>
              <Table verticalSpacing="sm" horizontalSpacing="md" striped highlightOnHover>
                <Table.Thead>
                  <Table.Tr>
                    <Table.Th style={{ borderBottom: "2px solid var(--border-color)" }}>
                      <Text size="sm" fw={700}>Worker ID</Text>
                    </Table.Th>
                    <Table.Th style={{ borderBottom: "2px solid var(--border-color)" }}>
                      <Text size="sm" fw={700}>Hostname</Text>
                    </Table.Th>
                    <Table.Th style={{ borderBottom: "2px solid var(--border-color)" }}>
                      <Text size="sm" fw={700}>Status</Text>
                    </Table.Th>
                    <Table.Th style={{ borderBottom: "2px solid var(--border-color)" }}>
                      <Text size="sm" fw={700}>Pools</Text>
                    </Table.Th>
                    <Table.Th style={{ borderBottom: "2px solid var(--border-color)" }}>
                      <Text size="sm" fw={700}>Capacity</Text>
                    </Table.Th>
                    <Table.Th style={{ borderBottom: "2px solid var(--border-color)" }}>
                      <Text size="sm" fw={700}>Last Heartbeat</Text>
                    </Table.Th>
                  </Table.Tr>
                </Table.Thead>
                <Table.Tbody>
                  {workers.map((worker) => (
                    <Table.Tr key={worker.worker_id}>
                      <Table.Td>
                        <Text size="sm" fw={500} ff="monospace">
                          {worker.worker_id}
                        </Text>
                      </Table.Td>
                      <Table.Td>
                        <Text size="sm">{worker.hostname}</Text>
                      </Table.Td>
                      <Table.Td>
                        <Badge
                          color={worker.status === "online" ? "green" : "red"}
                          variant="light"
                          size="sm"
                          radius="sm"
                          leftSection={
                            worker.status === "online" ? (
                              <IconCircleCheck size={12} style={{ display: "flex", alignItems: "center", marginTop: "2px" }} />
                            ) : (
                              <IconCircleX size={12} style={{ display: "flex", alignItems: "center", marginTop: "2px" }} />
                            )
                          }
                        >
                          {worker.status.toUpperCase()}
                        </Badge>
                      </Table.Td>
                      <Table.Td>
                        <Group gap={4}>
                          {(worker.pools || []).map((pool) => (
                            <Badge key={pool} variant="outline" color="teal" size="sm" radius="sm">
                              {pool}
                            </Badge>
                          ))}
                        </Group>
                      </Table.Td>
                      <Table.Td>
                        <Text size="sm">{worker.max_tasks} tasks</Text>
                      </Table.Td>
                      <Table.Td>
                        <Group gap={4}>
                          <IconClockHour4 size={14} color="var(--mantine-color-dimmed)" />
                          <Text size="sm" c="dimmed">
                            {timeSince(worker.last_seen)}
                          </Text>
                        </Group>
                      </Table.Td>
                    </Table.Tr>
                  ))}
                  {workers.length === 0 && (
                    <Table.Tr>
                      <Table.Td colSpan={6}>
                        <Text c="dimmed" ta="center" py="md">
                          No workers registered.
                        </Text>
                      </Table.Td>
                    </Table.Tr>
                  )}
                </Table.Tbody>
              </Table>
            </Table.ScrollContainer>
          </Card>
        </>
      )}
    </>
  );
}
