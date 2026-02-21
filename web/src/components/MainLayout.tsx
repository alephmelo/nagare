"use client";

import { AppShell, Burger, Group, Title, ActionIcon, useMantineColorScheme, useComputedColorScheme, Text, Container, Box, NavLink } from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import { IconActivity, IconSun, IconMoon, IconDashboard, IconSitemap, IconHistory } from "@tabler/icons-react";
import { useRouter, usePathname } from "next/navigation";
import { useEffect, useState } from "react";

export default function MainLayout({ children }: { children: React.ReactNode }) {
  const [opened, { toggle }] = useDisclosure();
  const { setColorScheme } = useMantineColorScheme();
  const computedColorScheme = useComputedColorScheme("dark", { getInitialValueInEffect: true });
  const [mounted, setMounted] = useState(false);
  // eslint-disable-next-line react-hooks/set-state-in-effect
  useEffect(() => { setMounted(true); }, []);
  const router = useRouter();
  const pathname = usePathname();

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
            <Title order={3} fw={700} c="cyan">Nagare</Title>
          </Group>
          <ActionIcon
            onClick={() => setColorScheme(computedColorScheme === "dark" ? "light" : "dark")}
            variant="default"
            size="lg"
            aria-label="Toggle color scheme"
          >
            {mounted
              ? (computedColorScheme === "dark" ? <IconSun size={18} /> : <IconMoon size={18} />)
              : <IconMoon size={18} />}
          </ActionIcon>
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
      </AppShell.Navbar>

      <AppShell.Main>
        <Container fluid>
          {children}
        </Container>
      </AppShell.Main>
    </AppShell>
  );
}
