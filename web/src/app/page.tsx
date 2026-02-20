"use client";

import { useEffect, useState } from "react";
import { AppShell, Burger, Group, Title, Container, Table, Badge, Card, Text, ActionIcon, Stack } from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import { IconRefresh, IconActivity } from "@tabler/icons-react";

export default function Dashboard() {
  const [opened, { toggle }] = useDisclosure();
  const [runs, setRuns] = useState<any[]>([]);
  const [dags, setDags] = useState<any[]>([]);

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
    }
  };

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 3000); // poll every 3 seconds
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
    <AppShell
      header={{ height: 60 }}
      navbar={{
        width: 300,
        breakpoint: "sm",
        collapsed: { mobile: !opened },
      }}
      padding="md"
    >
      <AppShell.Header>
        <Group h="100%" px="md" justify="space-between">
          <Group>
            <Burger opened={opened} onClick={toggle} hiddenFrom="sm" size="sm" />
            <IconActivity size={28} color="cyan" />
            <Title order={3}>Nagare</Title>
          </Group>
          <ActionIcon variant="light" onClick={fetchData}>
             <IconRefresh size={18} />
          </ActionIcon>
        </Group>
      </AppShell.Header>

      <AppShell.Navbar p="md">
        <Title order={5} mb="md">Loaded DAGs</Title>
        <Stack gap="sm">
          {dags?.map(dag => (
            <Card key={dag.ID} shadow="xs" padding="sm" radius="md" withBorder>
              <Text fw={500}>{dag.ID}</Text>
              <Text size="xs" c="dimmed">{dag.Description}</Text>
              <Badge mt="sm" size="xs" variant="light">{dag.Schedule}</Badge>
            </Card>
          ))}
        </Stack>
      </AppShell.Navbar>

      <AppShell.Main>
        <Container fluid>
          <Title order={4} mb="lg">Recent Runs</Title>
          <Card shadow="sm" radius="md" withBorder>
            <Table striped highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Run ID</Table.Th>
                  <Table.Th>DAG ID</Table.Th>
                  <Table.Th>Status</Table.Th>
                  <Table.Th>Execution Date</Table.Th>
                  <Table.Th>Created At</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {runs?.map((run) => (
                  <Table.Tr key={run.ID}>
                    <Table.Td>
                      <Badge 
                        variant="dot" 
                        size="lg" 
                        style={{ cursor: "pointer" }}
                        onClick={() => window.location.href = `/runs/?id=${run.ID}`}
                      >
                        {run.ID}
                      </Badge>
                    </Table.Td>
                    <Table.Td fw={500}>{run.DAGID}</Table.Td>
                    <Table.Td>
                      <Badge color={getStatusColor(run.Status)} variant="light">
                        {run.Status}
                      </Badge>
                    </Table.Td>
                    <Table.Td>{new Date(run.ExecDate).toLocaleString()}</Table.Td>
                    <Table.Td>{new Date(run.CreatedAt).toLocaleString()}</Table.Td>
                  </Table.Tr>
                ))}
                {(!runs || runs.length === 0) && (
                   <Table.Tr>
                     <Table.Td colSpan={5} align="center">
                        <Text c="dimmed">No runs found.</Text>
                     </Table.Td>
                   </Table.Tr>
                )}
              </Table.Tbody>
            </Table>
          </Card>
        </Container>
      </AppShell.Main>
    </AppShell>
  );
}
