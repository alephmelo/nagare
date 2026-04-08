"use client";

import {
  AppShell,
  Burger,
  Group,
  Title,
  ActionIcon,
  useMantineColorScheme,
  useComputedColorScheme,
  Text,
  Container,
  Box,
  NavLink,
  Tooltip,
  Divider,
} from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import {
  IconSun,
  IconMoon,
  IconDashboard,
  IconSitemap,
  IconHistory,
  IconLogout,
  IconServer,
  IconChartBar,
  IconCloudComputing,
} from "@tabler/icons-react";
import { useRouter, usePathname } from "next/navigation";
import { useEffect, useState } from "react";
import { useAuthContext } from "./AuthProvider";
import { type Icon } from "@tabler/icons-react";

interface NavItem {
  href: string;
  label: string;
  icon: Icon;
}

interface NavSection {
  label: string;
  items: NavItem[];
}

const NAV_SECTIONS: NavSection[] = [
  {
    label: "General",
    items: [{ href: "/", label: "Dashboard", icon: IconDashboard }],
  },
  {
    label: "Workflows",
    items: [
      { href: "/dags", label: "DAGs", icon: IconSitemap },
      { href: "/runs", label: "Runs", icon: IconHistory },
    ],
  },
  {
    label: "Cluster",
    items: [
      { href: "/workers", label: "Workers", icon: IconServer },
      { href: "/autoscaler", label: "Autoscaler", icon: IconCloudComputing },
    ],
  },
  {
    label: "Observability",
    items: [{ href: "/metrics", label: "Metrics", icon: IconChartBar }],
  },
];

export default function MainLayout({ children }: { children: React.ReactNode }) {
  const [opened, { toggle }] = useDisclosure();
  const { setColorScheme } = useMantineColorScheme();
  const computedColorScheme = useComputedColorScheme("dark", { getInitialValueInEffect: true });
  const [mounted, setMounted] = useState(false);
  useEffect(() => {
    setMounted(true);
  }, []);
  const router = useRouter();
  const pathname = usePathname();
  const { apiKey, clearApiKey } = useAuthContext();

  const isActive = (href: string) => (href === "/" ? pathname === "/" : pathname.startsWith(href));

  return (
    <AppShell
      header={{ height: 60 }}
      navbar={{
        width: 250,
        breakpoint: "sm",
        collapsed: { mobile: !opened },
      }}
      padding="md"
    >
      <AppShell.Header>
        <Group h="100%" px="md" justify="space-between">
          <Group>
            <Burger opened={opened} onClick={toggle} hiddenFrom="sm" size="sm" />
            <Group gap="xs" style={{ cursor: "pointer" }} onClick={() => router.push("/")}>
              <Text
                fw={900}
                c="blue.5"
                style={{ fontSize: "36px", lineHeight: 1, letterSpacing: "-1px" }}
              >
                流れ
              </Text>
              <Title order={3} fw={800} style={{ letterSpacing: "-0.5px" }}>
                NAGARE
              </Title>
            </Group>
          </Group>
          <Group gap="xs">
            <ActionIcon
              onClick={() => setColorScheme(computedColorScheme === "dark" ? "light" : "dark")}
              variant="default"
              size="lg"
              aria-label="Toggle color scheme"
            >
              {mounted ? (
                computedColorScheme === "dark" ? (
                  <IconSun size={18} />
                ) : (
                  <IconMoon size={18} />
                )
              ) : (
                <IconMoon size={18} />
              )}
            </ActionIcon>
          </Group>
        </Group>
      </AppShell.Header>

      <AppShell.Navbar p="md">
        {NAV_SECTIONS.map((section, idx) => (
          <Box key={section.label} mb="md">
            {idx > 0 && <Divider opacity={0.3} mb="md" />}
            <Text size="xs" fw={500} c="dimmed" mb="sm" tt="uppercase">
              {section.label}
            </Text>
            {section.items.map((item) => (
              <NavLink
                key={item.href}
                href={item.href}
                label={item.label}
                leftSection={<item.icon size="1rem" stroke={1.5} />}
                active={isActive(item.href)}
                onClick={(e) => {
                  e.preventDefault();
                  router.push(item.href);
                }}
                variant="filled"
              />
            ))}
          </Box>
        ))}

        {/* Only show disconnect when a key is actively stored */}
        {apiKey && (
          <Box mt="auto">
            <Tooltip label="Clear API key and return to login screen" position="right">
              <NavLink
                label="Disconnect"
                leftSection={<IconLogout size="1rem" stroke={1.5} />}
                onClick={clearApiKey}
                c="red"
                variant="subtle"
              />
            </Tooltip>
          </Box>
        )}
      </AppShell.Navbar>

      <AppShell.Main>
        <Container fluid>{children}</Container>
      </AppShell.Main>
    </AppShell>
  );
}
