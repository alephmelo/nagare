"use client";

import { useEffect, useState, Suspense } from "react";
import { useSearchParams, useRouter } from "next/navigation";
import { Title, Card, Code, Table, Badge, Button, Group, Text, Loader, Center } from "@mantine/core";
import { IconArrowLeft, IconRefresh } from "@tabler/icons-react";
import { useCallback } from "react";

interface RunTask {
  ID: string;
  TaskID: string;
  Status: string;
  Output: string;
  UpdatedAt: string;
}

function RunDetailsContent() {
  const searchParams = useSearchParams();
  const id = searchParams.get('id');
  const router = useRouter();
  const [tasks, setTasks] = useState<RunTask[]>([]);
  const [loading, setLoading] = useState(true);

  const fetchTasks = useCallback(async () => {
    if (!id) return;
    try {
      setLoading(true);
      const res = await fetch(`/api/runs/${id}/tasks`);
      if (res.ok) {
        setTasks(await res.json());
      }
    } catch (err) {
      console.error("Failed to fetch tasks", err);
    } finally {
      setLoading(false);
    }
  }, [id]);

  useEffect(() => {
    fetchTasks();
    const interval = setInterval(fetchTasks, 3000);
    return () => clearInterval(interval);
  }, [fetchTasks]);

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
     return <Center h={200}><Title order={3} c="dimmed">No RUN ID provided</Title></Center>;
  }

  return (
    <>
      <Group justify="space-between" mb="xl">
        <Group>
          <Button variant="subtle" color="gray" leftSection={<IconArrowLeft size={16} />} onClick={() => router.push("/")}>
            Back
          </Button>
          <Title order={3}>Run Details</Title>
          <Badge size="lg" variant="outline" color="cyan" radius="sm">{id}</Badge>
        </Group>
        <Button variant="light" leftSection={<IconRefresh size={16} />} onClick={fetchTasks} loading={loading}>
          Refresh
        </Button>
      </Group>

      <Card shadow="sm" radius="md" withBorder padding="0" style={{ overflow: 'hidden' }}>
        <Table.ScrollContainer minWidth={800}>
          <Table striped highlightOnHover verticalSpacing="sm">
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
                      {task.Status.toUpperCase()}
                    </Badge>
                  </Table.Td>
                  <Table.Td style={{ maxWidth: '400px' }}>
                    <Code block style={{ whiteSpace: 'pre-wrap', maxHeight: '200px', overflowY: 'auto', backgroundColor: 'var(--mantine-color-dark-8)' }}>
                      {task.Output || "No output generated yet."}
                    </Code>
                  </Table.Td>
                  <Table.Td>{new Date(task.UpdatedAt).toLocaleString()}</Table.Td>
                </Table.Tr>
              ))}
              {(!tasks || tasks.length === 0) && !loading && (
                  <Table.Tr>
                    <Table.Td colSpan={4} align="center" py="xl">
                      <Text c="dimmed">No tasks found for this run.</Text>
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

export default function RunDetails() {
  return (
    <Suspense fallback={<Center h={200}><Loader color="cyan" /></Center>}>
      <RunDetailsContent />
    </Suspense>
  );
}
