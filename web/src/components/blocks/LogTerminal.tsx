import { Box, Group, Text, Code } from "@mantine/core";
import { IconTerminal2 } from "@tabler/icons-react";
import { forwardRef } from "react";

interface LogTerminalProps {
  label?: string;
  isLive?: boolean;
  content: string;
  isFailed?: boolean;
}

export const LogTerminal = forwardRef<HTMLElement, LogTerminalProps>(
  ({ label = "Output Log", isLive, content, isFailed }, ref) => {
    return (
      <Box mb="md">
        <style>{`
          @keyframes pulse {
            0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(34, 139, 230, 0.7); }
            70% { transform: scale(1); box-shadow: 0 0 0 6px rgba(34, 139, 230, 0); }
            100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(34, 139, 230, 0); }
          }
        `}</style>
        <Group justify="space-between" mb="xs">
          <Group gap="xs">
            <Group gap={6} mr="sm">
              <Box
                w={10}
                h={10}
                style={{ borderRadius: "50%", backgroundColor: "var(--mantine-color-red-5)" }}
              />
              <Box
                w={10}
                h={10}
                style={{ borderRadius: "50%", backgroundColor: "var(--mantine-color-yellow-5)" }}
              />
              <Box
                w={10}
                h={10}
                style={{ borderRadius: "50%", backgroundColor: "var(--mantine-color-green-5)" }}
              />
            </Group>
            <IconTerminal2 size={14} color="var(--mantine-color-dimmed)" />
            <Text size="xs" c="dimmed" tt="uppercase" fw={700} style={{ letterSpacing: "1px" }}>
              {label}
            </Text>
          </Group>
          {isLive && (
            <Group gap={6}>
              <Box
                w={8}
                h={8}
                style={{
                  borderRadius: "50%",
                  backgroundColor: "var(--mantine-color-blue-5)",
                  animation: "pulse 1.5s infinite",
                  boxShadow: "0 0 8px var(--mantine-color-blue-filled)",
                }}
              />
              <Text span size="xs" c="blue" fw={500}>
                Live
              </Text>
            </Group>
          )}
        </Group>

        <Code
          ref={ref}
          block
          style={{
            whiteSpace: "pre-wrap",
            maxHeight: "300px",
            overflowY: "auto",
            fontSize: "12px",
            lineHeight: 1.7,
            backgroundColor: "var(--mantine-color-dark-8)",
            border: "1px solid var(--mantine-color-dark-4)",
            color: isFailed ? "var(--mantine-color-red-4)" : "var(--mantine-color-gray-4)",
            borderRadius: "var(--mantine-radius-md)",
            padding: "var(--mantine-spacing-md)",
            boxShadow: "inset 0 2px 10px rgba(0,0,0,0.2)",
          }}
        >
          {content}
        </Code>
      </Box>
    );
  }
);

LogTerminal.displayName = "LogTerminal";
