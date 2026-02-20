"use client";

import { useEffect, useState, Suspense } from "react";
import { useSearchParams, useRouter } from "next/navigation";
import { Title, Card, Code, Table, Badge, Button, Group, Text, Loader, Center, ActionIcon } from "@mantine/core";
import { IconArrowLeft, IconRefresh, IconPlayerPlay } from "@tabler/icons-react";
import { useCallback } from "react";
import { notifications } from '@mantine/notifications';

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

  const handleRetry = async (taskID: string) => {
    try {
      const res = await fetch(`/api/runs/${id}/tasks/${taskID}/retry`, { method: "POST" });
      if (res.ok) {
        fetchTasks();
        notifications.show({
          title: 'Task Requeued',
          message: `Successfully sent task ${taskID} back to pending state.`,
          color: 'green',
        });
      } else {
        notifications.show({
          title: 'Retry Failed',
          message: `Failed to queue task ${taskID} for retry.`,
          color: 'red',
        });
      }
    } catch (err) {
      console.error(err);
      notifications.show({
        title: 'Network Error',
        message: 'Could not communicate with the API.',
        color: 'red',
      });
    }
  };

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
          <Badge size="lg" variant="outline" color="blue">{id}</Badge>
        </Group>
        <Button variant="light" leftSection={<IconRefresh size={16} />} onClick={fetchTasks} loading={loading}>
           Refresh
        </Button>
      </Group>

      <Card padding="0" style={{ overflow: 'hidden' }}>
        <Table.ScrollContainer minWidth={800}>
          <Table striped highlightOnHover verticalSpacing="md" horizontalSpacing="md">
            <Table.Thead>
              <Table.Tr>
                <Table.Th style={{ borderBottom: '2px solid var(--border-color)' }}>Task ID</Table.Th>
                <Table.Th style={{ borderBottom: '2px solid var(--border-color)' }}>Status</Table.Th>
                <Table.Th style={{ borderBottom: '2px solid var(--border-color)' }}>Output Log</Table.Th>
                <Table.Th style={{ borderBottom: '2px solid var(--border-color)' }}>Updated At</Table.Th>
                <Table.Th style={{ borderBottom: '2px solid var(--border-color)' }}>Action</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {tasks?.map((task) => (
                <Table.Tr key={task.ID}>
                  <Table.Td fw={500}>{task.TaskID}</Table.Td>
                  <Table.Td>
                    <Badge color={getStatusColor(task.Status)} variant="light" radius="sm">
                      {task.Status.toUpperCase()}
                    </Badge>
                  </Table.Td>
                  <Table.Td style={{ maxWidth: '400px' }}>
                    <Code block style={{ whiteSpace: 'pre-wrap', maxHeight: '200px', overflowY: 'auto', backgroundColor: 'rgba(0,0,0,0.3)', border: '1px solid var(--border-color)', color: '#d1d5db' }}>
                      {task.Output || "No output generated yet."}
                    </Code>
                  </Table.Td>
                  <Table.Td>{new Date(task.UpdatedAt).toLocaleString()}</Table.Td>
                  <Table.Td>
                    {(task.Status === "success" || task.Status === "failed") && (
                      <ActionIcon 
                         variant="light" 
                         color="blue" 
                         onClick={() => handleRetry(task.TaskID)}
                         title="Retry Task"
                      >
                         <IconPlayerPlay size={16} />
                      </ActionIcon>
                    )}
                  </Table.Td>
                </Table.Tr>
              ))}
              {(!tasks || tasks.length === 0) && !loading && (
                  <Table.Tr>
                    <Table.Td colSpan={5} align="center" py="xl">
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
