import { Group, Title, Button, Text } from "@mantine/core";
import { IconArrowLeft } from "@tabler/icons-react";
import { useRouter } from "next/navigation";

interface PageHeaderProps {
  title: string;
  subtitle?: string;
  badge?: React.ReactNode;
  showBack?: boolean;
  backTo?: string;
  actions?: React.ReactNode;
}

export function PageHeader({
  title,
  subtitle,
  badge,
  showBack,
  backTo = "/",
  actions,
}: PageHeaderProps) {
  const router = useRouter();

  return (
    <Group justify="space-between" mb="xl">
      <Group>
        {showBack && (
          <Button
            variant="subtle"
            color="gray"
            leftSection={<IconArrowLeft size={16} />}
            onClick={() => router.push(backTo)}
          >
            Back
          </Button>
        )}
        <div>
          <Group gap="xs" align="center">
            <Title order={2} style={subtitle ? { marginBottom: -4 } : undefined}>
              {title}
            </Title>
            {badge && badge}
          </Group>
          {subtitle && (
            <Text size="xs" c="dimmed" style={{ fontFamily: "monospace", marginTop: 4 }}>
              {subtitle}
            </Text>
          )}
        </div>
      </Group>
      {actions && <Group gap="sm">{actions}</Group>}
    </Group>
  );
}
