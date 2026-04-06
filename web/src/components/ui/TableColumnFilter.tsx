import { Menu, UnstyledButton, Group, Text } from "@mantine/core";
import { IconFilter, IconCheck } from "@tabler/icons-react";

interface Option {
  value: string;
  label: string;
}

interface TableColumnFilterProps {
  label: string;
  menuLabel: string;
  options: Option[];
  value: string;
  onChange: (val: string) => void;
  width?: number;
}

export function TableColumnFilter({
  label,
  menuLabel,
  options,
  value,
  onChange,
  width = 150,
}: TableColumnFilterProps) {
  const isActive = value !== "all";

  return (
    <Menu shadow="md" width={width}>
      <Menu.Target>
        <UnstyledButton>
          <Group gap={4}>
            <Text
              size="xs"
              fw={600}
              tt="uppercase"
              c={isActive ? "blue" : "dimmed"}
              style={{ letterSpacing: "1px", transition: "color 0.2s" }}
            >
              {label}
            </Text>
            <IconFilter
              size={14}
              color={isActive ? "var(--mantine-color-blue-filled)" : "var(--mantine-color-gray-5)"}
              style={{ transition: "color 0.2s" }}
            />
          </Group>
        </UnstyledButton>
      </Menu.Target>
      <Menu.Dropdown>
        <Menu.Label>{menuLabel}</Menu.Label>
        {options.map((opt) => (
          <Menu.Item
            key={opt.value}
            onClick={() => onChange(opt.value)}
            leftSection={
              value === opt.value ? <IconCheck size={14} /> : <div style={{ width: 14 }} />
            }
          >
            {opt.label}
          </Menu.Item>
        ))}
      </Menu.Dropdown>
    </Menu>
  );
}
