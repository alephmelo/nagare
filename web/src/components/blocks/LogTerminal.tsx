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
        <Group gap="xs" mb="xs">
          <IconTerminal2 size={14} color="var(--mantine-color-dimmed)" />
          <Text size="xs" c="dimmed" tt="uppercase" fw={700}>
            {label}
            {isLive && (
              <>
                {" "}
                &mdash;{" "}
                <Text span size="xs" c="blue" fw={400} style={{ animation: "pulse 2s infinite" }}>
                  streaming live
                </Text>
              </>
            )}
          </Text>
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
            border: "1px solid var(--mantine-color-dark-5)",
            color: isFailed ? "var(--mantine-color-red-4)" : "var(--mantine-color-gray-3)",
            borderRadius: "var(--mantine-radius-md)",
            padding: "var(--mantine-spacing-sm)",
          }}
        >
          {content}
        </Code>
      </Box>
    );
  }
);

LogTerminal.displayName = "LogTerminal";
