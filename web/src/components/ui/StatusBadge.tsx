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
  return (
    <Badge
      color={getStatusColor(status)}
      variant="light"
      size={size}
      className={className}
      radius="sm"
      style={
        animated
          ? {
              transition: "transform 0.2s ease, filter 0.2s ease",
              cursor: "default",
            }
          : undefined
      }
      onMouseEnter={
        animated
          ? (e) => {
              e.currentTarget.style.transform = "scale(1.05)";
              e.currentTarget.style.filter = "brightness(1.1)";
            }
          : undefined
      }
      onMouseLeave={
        animated
          ? (e) => {
              e.currentTarget.style.transform = "scale(1)";
              e.currentTarget.style.filter = "brightness(1)";
            }
          : undefined
      }
    >
      {status.toUpperCase()}
    </Badge>
  );
}
