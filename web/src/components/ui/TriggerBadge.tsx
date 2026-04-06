import { Badge, MantineSize } from "@mantine/core";
import { IconUser, IconRobot, IconActivity } from "@tabler/icons-react";

interface TriggerBadgeProps {
  trigger: string;
  size?: MantineSize;
}

export function TriggerBadge({ trigger, size = "sm" }: TriggerBadgeProps) {
  const t = trigger.toLowerCase();

  if (t === "manual") {
    return (
      <Badge
        variant="light"
        color="blue"
        size={size}
        leftSection={
          <IconUser size={12} style={{ display: "flex", alignItems: "center", marginTop: "2px" }} />
        }
      >
        Manual
      </Badge>
    );
  }

  if (t === "scheduled") {
    return (
      <Badge
        variant="light"
        color="teal"
        size={size}
        leftSection={
          <IconRobot
            size={12}
            style={{ display: "flex", alignItems: "center", marginTop: "2px" }}
          />
        }
      >
        Scheduled
      </Badge>
    );
  }

  return (
    <Badge
      variant="light"
      color="violet"
      size={size}
      leftSection={
        <IconActivity
          size={12}
          style={{ display: "flex", alignItems: "center", marginTop: "2px" }}
        />
      }
    >
      Triggered
    </Badge>
  );
}
