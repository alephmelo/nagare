"use client";

import { useEffect, useState } from "react";
import { Title, Card, Table, Badge, SimpleGrid, Text, Group, Button, Skeleton } from "@mantine/core";
import { IconPlayerPlay, IconRefresh } from "@tabler/icons-react";
import { useRouter } from "next/navigation";

interface Run {
  ID: string;
  DAGID: string;
  Status: string;
  ExecDate: string;
  UpdatedAt: string;
  CreatedAt: string;
}

interface Dag {
  ID: string;
  Schedule: string;
  Description: string;
}

export default function Dashboard() {
  const [runs, setRuns] = useState<Run[]>([]);
  const [dags, setDags] = useState<Dag[]>([]);
  const [loading, setLoading] = useState(true);
  const router = useRouter();

  const fetchData = async () => {
    try {
      const [runsRes, dagsRes] = await Promise.all([
        fetch("/api/runs"),
        fetch("/api/dags")
      ]);
      setRuns(await runsRes.json());
      setDags(await dagsRes.json());
    } catch (err) {
      console.error("Failed to fetch data", err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 3000);
    return () => clearInterval(interval);
  }, []);

  const getStatusColor = (status: string) => {
    switch (status) {
      case "success": return "green";
      case "failed": return "red";
      case "running": return "blue";
      case "queued": return "yellow";
      default: return "gray";
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

      <Title order={4} mb="md" c="dimmed">Loaded Workflows</Title>
      {loading && dags.length === 0 ? (
        <SimpleGrid cols={{ base: 1, sm: 2, lg: 3 }} mb="xl">
          <Skeleton height={120} radius="md" />
          <Skeleton height={120} radius="md" />
          <Skeleton height={120} radius="md" />
        </SimpleGrid>
      ) : (
        <SimpleGrid cols={{ base: 1, sm: 2, lg: 3 }} mb="xl">
          {dags?.map(dag => (
            <Card key={dag.ID} shadow="sm" radius="md" padding="lg" withBorder>
              <Group justify="space-between" mt="0" mb="xs">
                <Text fw={600} size="lg">{dag.ID}</Text>
                <Badge variant="light" color="cyan">{dag.Schedule}</Badge>
              </Group>
              <Text size="sm" c="dimmed" mb="md" style={{ minHeight: '40px' }}>
                {dag.Description}
              </Text>
              <Button 
                variant="light" 
                color="blue" 
                fullWidth 
                mt="md" 
                radius="md" 
                leftSection={<IconPlayerPlay size={16} />}
                onClick={() => {
                   alert("Trigger API not currently implemented!");
                }}
              >
                Trigger
              </Button>
            </Card>
          ))}
          {(!dags || dags.length === 0) && (
            <Text c="dimmed">No DAGs loaded.</Text>
          )}
        </SimpleGrid>
      )}

      <Title order={4} mt="xl" mb="md" c="dimmed">Recent Runs</Title>
      <Card shadow="sm" radius="md" withBorder padding="0" style={{ overflow: "hidden" }}>
        <Table.ScrollContainer minWidth={800}>
          <Table verticalSpacing="sm" striped highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Run ID</Table.Th>
                <Table.Th>DAG ID</Table.Th>
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
                    <Badge variant="outline" color="gray">{run.DAGID}</Badge>
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
              {(!runs || runs.length === 0) && !loading && (
                  <Table.Tr>
                    <Table.Td colSpan={5} align="center" py="xl">
                      <Text c="dimmed">No recent runs found.</Text>
                    </Table.Td>
                  </Table.Tr>
              )}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      </Card>
    </>
  );
}
