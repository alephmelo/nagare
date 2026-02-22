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
} from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import {
  IconActivity,
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
            <IconActivity size={28} color="cyan" />
            <Title order={3} fw={700} c="cyan">
              Nagare
            </Title>
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
        <Box mb="md">
          <Text size="xs" fw={500} c="dimmed" mb="sm" tt="uppercase">
            General
          </Text>
          <NavLink
            href="/"
            label="Dashboard"
            leftSection={<IconDashboard size="1rem" stroke={1.5} />}
            active={pathname === "/"}
            onClick={(e) => {
              e.preventDefault();
              router.push("/");
            }}
            variant="filled"
          />
        </Box>
        <Box mb="md">
          <Text size="xs" fw={500} c="dimmed" mb="sm" tt="uppercase">
            Workflows
          </Text>
          <NavLink
            href="/dags"
            label="DAGs"
            leftSection={<IconSitemap size="1rem" stroke={1.5} />}
            active={pathname === "/dags"}
            onClick={(e) => {
              e.preventDefault();
              router.push("/dags");
            }}
            variant="filled"
          />
          <NavLink
            href="/runs"
            label="Runs"
            leftSection={<IconHistory size="1rem" stroke={1.5} />}
            active={pathname === "/runs"}
            onClick={(e) => {
              e.preventDefault();
              router.push("/runs");
            }}
            variant="filled"
          />
        </Box>
        <Box mb="md">
          <Text size="xs" fw={500} c="dimmed" mb="sm" tt="uppercase">
            Cluster
          </Text>
          <NavLink
            href="/workers"
            label="Workers"
            leftSection={<IconServer size="1rem" stroke={1.5} />}
            active={pathname === "/workers"}
            onClick={(e) => {
              e.preventDefault();
              router.push("/workers");
            }}
            variant="filled"
          />
          <NavLink
            href="/autoscaler"
            label="Autoscaler"
            leftSection={<IconCloudComputing size="1rem" stroke={1.5} />}
            active={pathname === "/autoscaler"}
            onClick={(e) => {
              e.preventDefault();
              router.push("/autoscaler");
            }}
            variant="filled"
          />
        </Box>
        <Box mb="md">
          <Text size="xs" fw={500} c="dimmed" mb="sm" tt="uppercase">
            Observability
          </Text>
          <NavLink
            href="/metrics"
            label="Metrics"
            leftSection={<IconChartBar size="1rem" stroke={1.5} />}
            active={pathname === "/metrics"}
            onClick={(e) => {
              e.preventDefault();
              router.push("/metrics");
            }}
            variant="filled"
          />
        </Box>

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
