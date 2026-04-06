import { Badge, MantineSize } from "@mantine/core";

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

interface StatusBadgeProps {
  status: string;
  size?: MantineSize;
  className?: string;
  animated?: boolean;
}

export function StatusBadge({ status, size = "sm", className, animated = true }: StatusBadgeProps) {
  const isRunning = status.toLowerCase() === "running";
  return (
    <Badge
      color={getStatusColor(status)}
      variant={isRunning ? "light" : "dot"}
      size={size}
      className={className}
      radius="md"
      style={
        animated
          ? {
              transition: "transform 0.15s ease, opacity 0.15s ease",
              cursor: "default",
            }
          : undefined
      }
      onMouseEnter={
        animated
          ? (e) => {
              e.currentTarget.style.transform = "scale(1.02)";
              e.currentTarget.style.opacity = "0.9";
            }
          : undefined
      }
      onMouseLeave={
        animated
          ? (e) => {
              e.currentTarget.style.transform = "scale(1)";
              e.currentTarget.style.opacity = "1";
            }
          : undefined
      }
    >
      {status.toUpperCase()}
    </Badge>
  );
}
