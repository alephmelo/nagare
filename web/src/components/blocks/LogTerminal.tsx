import { Box, Group, Text, Code, ActionIcon, Tooltip } from "@mantine/core";
import {
  IconTerminal2,
  IconCopy,
  IconCheck,
  IconMaximize,
  IconMinimize,
} from "@tabler/icons-react";
import { forwardRef, useState } from "react";

interface LogTerminalProps {
  label?: string;
  isLive?: boolean;
  content: string;
  isFailed?: boolean;
}

export const LogTerminal = forwardRef<HTMLElement, LogTerminalProps>(
  ({ label = "Output Log", isLive, content, isFailed }, ref) => {
    const [copied, setCopied] = useState(false);
    const [isExpanded, setExpanded] = useState(false);

    const handleCopy = async () => {
      await navigator.clipboard.writeText(content);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    };

    return (
      <Box mb="md">
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
          <Group gap={6}>
            {isLive && (
              <Group gap={6} mr="xs">
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
            <Tooltip label={copied ? "Copied!" : "Copy to clipboard"}>
              <ActionIcon
                variant="subtle"
                size="xs"
                color={copied ? "green" : "gray"}
                onClick={handleCopy}
              >
                {copied ? <IconCheck size={12} /> : <IconCopy size={12} />}
              </ActionIcon>
            </Tooltip>
            <Tooltip label={isExpanded ? "Collapse" : "Expand"}>
              <ActionIcon
                variant="subtle"
                size="xs"
                color="gray"
                onClick={() => setExpanded((e) => !e)}
              >
                {isExpanded ? <IconMinimize size={12} /> : <IconMaximize size={12} />}
              </ActionIcon>
            </Tooltip>
          </Group>
        </Group>

        <Code
          ref={ref}
          block
          style={{
            whiteSpace: "pre-wrap",
            maxHeight: isExpanded ? "none" : "300px",
            overflowY: "auto",
            fontSize: "12px",
            lineHeight: 1.7,
            backgroundColor: "var(--log-bg)",
            border: "1px solid var(--log-border)",
            color: isFailed ? "var(--log-text-failed)" : "var(--log-text-default)",
            borderRadius: "var(--mantine-radius-md)",
            padding: "var(--mantine-spacing-md)",
            boxShadow: "inset 0 2px 10px rgba(0,0,0,0.08)",
          }}
        >
          {content}
        </Code>
      </Box>
    );
  }
);

LogTerminal.displayName = "LogTerminal";
