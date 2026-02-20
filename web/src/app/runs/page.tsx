"use client";

import { useEffect, useState, Suspense } from "react";
import { useSearchParams, useRouter } from "next/navigation";
import { AppShell, Container, Title, Card, Code, Table, Badge, Button, Group } from "@mantine/core";
import { IconArrowLeft } from "@tabler/icons-react";

function RunDetailsContent() {
  const searchParams = useSearchParams();
  const id = searchParams.get('id');
  const router = useRouter();
  const [tasks, setTasks] = useState<any[]>([]);

  const fetchTasks = async () => {
    if (!id) return;
    try {
      const res = await fetch(`/api/runs/${id}/tasks`);
      if (res.ok) {
        setTasks(await res.json());
      }
    } catch (err) {
      console.error("Failed to fetch tasks", err);
    }
  };

  useEffect(() => {
    fetchTasks();
    const interval = setInterval(fetchTasks, 3000);
    return () => clearInterval(interval);
  }, [id]);

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
     return <Title>No RUN ID provided</Title>;
  }

  return (
    <>
      <Group mb="lg">
        <Button variant="subtle" leftSection={<IconArrowLeft size={16} />} onClick={() => router.push("/")}>
          Back to Dashboard
        </Button>
        <Title order={3}>Run Details: {id}</Title>
      </Group>

      <Card shadow="sm" radius="md" withBorder>
        <Title order={5} mb="md">Task Execution Logs</Title>
        <Table striped highlightOnHover>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>Task ID</Table.Th>
              <Table.Th>Status</Table.Th>
              <Table.Th>Output Log</Table.Th>
              <Table.Th>Updated At</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {tasks?.map((task) => (
              <Table.Tr key={task.ID}>
                <Table.Td fw={500}>{task.TaskID}</Table.Td>
                <Table.Td>
                  <Badge color={getStatusColor(task.Status)} variant="light">
                    {task.Status}
                  </Badge>
                </Table.Td>
                <Table.Td style={{ maxWidth: '400px' }}>
                  <Code block style={{ whiteSpace: 'pre-wrap', maxHeight: '200px', overflowY: 'auto' }}>
                    {task.Output || "No output generated yet."}
                  </Code>
                </Table.Td>
                <Table.Td>{new Date(task.UpdatedAt).toLocaleString()}</Table.Td>
              </Table.Tr>
            ))}
            {(!tasks || tasks.length === 0) && (
                <Table.Tr>
                  <Table.Td colSpan={4} align="center">
                    No tasks found for this run.
                  </Table.Td>
                </Table.Tr>
            )}
          </Table.Tbody>
        </Table>
      </Card>
    </>
  );
}

export default function RunDetails() {
  return (
    <AppShell padding="md">
      <AppShell.Main>
        <Container fluid>
           <Suspense fallback={<div>Loading...</div>}>
             <RunDetailsContent />
           </Suspense>
        </Container>
      </AppShell.Main>
    </AppShell>
  );
}
