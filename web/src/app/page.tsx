"use client";

import { useEffect, useState } from "react";
import { Title, Card, Table, Badge, SimpleGrid, Text, Group, Button, Skeleton, Select, Pagination, RingProgress, Center, Alert, List } from "@mantine/core";
import { IconPlayerPlay, IconRefresh, IconCheck, IconX, IconActivity, IconAlertCircle, IconArrowRight } from "@tabler/icons-react";
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
  const [dagErrors, setDagErrors] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(true);
  
  // Pagination & Filtering state
  const [page, setPage] = useState(1);
  const [dagFilter, setDagFilter] = useState<string | null>("all");
  const [totalRuns, setTotalRuns] = useState(0);
  const limit = 10;
  
  const router = useRouter();

  const fetchData = async () => {
    try {
      setLoading(true);
      const [runsRes, dagsRes, errorsRes] = await Promise.all([
        fetch(`/api/runs?page=${page}&limit=${limit}&dag_id=${dagFilter || "all"}`),
        fetch("/api/dags"),
        fetch("/api/dags/errors")
      ]);
      
      const runsData = await runsRes.json();
      setRuns(runsData.data || []);
      setTotalRuns(runsData.total || 0);
      
      setDags(await dagsRes.json());
      setDagErrors(await errorsRes.json() || {});
    } catch (err) {
      console.error("Failed to fetch data", err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, [page, dagFilter]);

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

      {Object.keys(dagErrors).length > 0 && (
        <Alert 
          variant="light" 
          color="red" 
          title="DAG Validation Errors" 
          icon={<IconAlertCircle />} 
          mb="xl"
          p="md"
        >
          <Text size="sm" mb="xs">
            The following DAG configurations have problems and were safely skipped by the scheduler:
          </Text>
          <List size="sm" spacing="xs">
            {Object.entries(dagErrors).map(([file, err]) => (
              <List.Item key={file}>
                <strong>{file}</strong>: <Text span c="dimmed" fs="italic">{err}</Text>
              </List.Item>
            ))}
          </List>
        </Alert>
      )}

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
                leftSection={<IconArrowRight size={16} />}
                onClick={() => {
                   router.push(`/dags?id=${dag.ID}`);
                }}
              >
                View Details
              </Button>
            </Card>
          ))}
          {(!dags || dags.length === 0) && (
            <Text c="dimmed">No DAGs loaded.</Text>
          )}
        </SimpleGrid>
      )}

      <Group justify="space-between" mt="xl" mb="md">
        <Title order={4} c="dimmed">Recent Runs</Title>
        <Select
          placeholder="Filter by DAG"
          data={[
            { value: "all", label: "All DAGs" },
            ...dags.map(d => ({ value: d.ID, label: d.ID }))
          ]}
          value={dagFilter}
          onChange={(val) => {
            setDagFilter(val);
            setPage(1); // Reset to page 1 on filter change
          }}
          disabled={loading && dags.length === 0}
          clearable={false}
          style={{ width: 250 }}
        />
      </Group>

      {/* Metrics Cards */}
      <SimpleGrid cols={{ base: 1, sm: 3 }} mb="xl">
        <Card shadow="sm" radius="md" withBorder padding="md">
           <Group>
             <RingProgress
                size={80}
                roundCaps
                thickness={8}
                sections={[{ value: 100, color: 'blue' }]}
                label={<Center><IconActivity size={20} /></Center>}
             />
             <div>
               <Text c="dimmed" size="xs" tt="uppercase" fw={700}>Total Runs Recorded</Text>
               <Text fw={700} size="xl">{totalRuns}</Text>
             </div>
           </Group>
        </Card>
        
        <Card shadow="sm" radius="md" withBorder padding="md">
           <Group>
             <RingProgress
                size={80}
                roundCaps
                thickness={8}
                sections={[{ value: runs.length > 0 ? (runs.filter(r => r.Status === 'success').length / runs.length) * 100 : 0, color: 'teal' }]}
                label={<Center><IconCheck size={20} color="teal" /></Center>}
             />
             <div>
               <Text c="dimmed" size="xs" tt="uppercase" fw={700}>Page Success Rate</Text>
               <Text fw={700} size="xl">
                 {runs.length > 0 ? Math.round((runs.filter(r => r.Status === 'success').length / runs.length) * 100) : 0}%
               </Text>
             </div>
           </Group>
        </Card>

        <Card shadow="sm" radius="md" withBorder padding="md">
           <Group>
             <RingProgress
                size={80}
                roundCaps
                thickness={8}
                sections={[{ value: runs.length > 0 ? (runs.filter(r => r.Status === 'failed').length / runs.length) * 100 : 0, color: 'red' }]}
                label={<Center><IconX size={20} color="red" /></Center>}
             />
             <div>
               <Text c="dimmed" size="xs" tt="uppercase" fw={700}>Page Failure Rate</Text>
               <Text fw={700} size="xl">
                 {runs.length > 0 ? Math.round((runs.filter(r => r.Status === 'failed').length / runs.length) * 100) : 0}%
               </Text>
             </div>
           </Group>
        </Card>
      </SimpleGrid>

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
                      <Text c="dimmed">No runs found for this configuration.</Text>
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
    </>
  );
}
