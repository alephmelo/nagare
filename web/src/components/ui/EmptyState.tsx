import { Card, Center, Text } from "@mantine/core";
import { IconDatabaseOff } from "@tabler/icons-react";

interface EmptyStateProps {
  title?: string;
  description?: string;
}

export function EmptyState({ title = "No data found", description }: EmptyStateProps) {
  return (
    <Card padding="xl" radius="md">
      <Center style={{ flexDirection: "column" }} py="xl">
        <IconDatabaseOff
          size={48}
          color="var(--mantine-color-gray-4)"
          stroke={1.5}
          style={{ marginBottom: 16 }}
        />
        <Text c="dimmed" fw={500} size="lg">
          {title}
        </Text>
        {description && (
          <Text c="dimmed" size="sm">
            {description}
          </Text>
        )}
      </Center>
    </Card>
  );
}
