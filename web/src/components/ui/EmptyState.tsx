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
          color="var(--mantine-color-gray-5)"
          stroke={1.5}
          style={{
            marginBottom: 16,
            animation: "float 4s ease-in-out infinite",
          }}
        />
        <style>{`
          @keyframes float {
            0% { transform: translateY(0px); }
            50% { transform: translateY(-8px); }
            100% { transform: translateY(0px); }
          }
        `}</style>
        <Text c="dimmed" fw={600} size="lg">
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
