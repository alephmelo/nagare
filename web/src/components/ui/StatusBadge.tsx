import { Badge, MantineSize } from "@mantine/core";

const STATUS_LABELS: Record<string, string> = {
  success: "Success",
  failed: "Failed",
  running: "Running",
  queued: "Queued",
  up_for_retry: "Retrying",
  cancelled: "Cancelled",
};

export function getStatusColor(status: string) {
  switch (status.toLowerCase()) {
    case "success":
      return "green";
    case "failed":
      return "red";
    case "running":
      return "blue";
    case "queued":
      return "yellow";
    case "up_for_retry":
      return "orange";
    case "cancelled":
      return "gray";
    default:
      return "gray";
  }
}

export function getStatusLabel(status: string) {
  return STATUS_LABELS[status.toLowerCase()] || status;
}

interface StatusBadgeProps {
  status: string;
  size?: MantineSize;
  className?: string;
}

export function StatusBadge({ status, size = "sm", className }: StatusBadgeProps) {
  const isRunning = status.toLowerCase() === "running";
  return (
    <Badge
      color={getStatusColor(status)}
      variant={isRunning ? "light" : "dot"}
      size={size}
      className={className}
      radius="xl"
    >
      {getStatusLabel(status)}
    </Badge>
  );
}
