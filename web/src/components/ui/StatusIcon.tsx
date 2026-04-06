import { ThemeIcon, Loader, MantineSize } from "@mantine/core";
import { IconCheck, IconX, IconClock, IconPlayerStop, IconAlertCircle } from "@tabler/icons-react";
import { getStatusColor } from "./StatusBadge";

interface StatusIconProps {
  status: string;
  size?: MantineSize | number;
}

export function StatusIcon({ status, size = "md" }: StatusIconProps) {
  const s = status.toLowerCase();

  if (s === "success") {
    return (
      <ThemeIcon color="green" variant="light" size={size} radius="xl">
        <IconCheck size={14} />
      </ThemeIcon>
    );
  }

  if (s === "failed") {
    return (
      <ThemeIcon color="red" variant="light" size={size} radius="xl">
        <IconX size={14} />
      </ThemeIcon>
    );
  }

  if (s === "running") {
    return (
      <ThemeIcon color="blue" variant="light" size={size} radius="xl">
        <Loader size={12} color="blue" />
      </ThemeIcon>
    );
  }

  if (s === "queued" || s === "up_for_retry") {
    return (
      <ThemeIcon color={getStatusColor(s)} variant="light" size={size} radius="xl">
        <IconClock size={14} />
      </ThemeIcon>
    );
  }

  if (s === "cancelled") {
    return (
      <ThemeIcon color="gray" variant="light" size={size} radius="xl">
        <IconPlayerStop size={14} />
      </ThemeIcon>
    );
  }

  return (
    <ThemeIcon color="gray" variant="light" size={size} radius="xl">
      <IconAlertCircle size={14} />
    </ThemeIcon>
  );
}
