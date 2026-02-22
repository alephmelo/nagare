"use client";

import React, { createContext, useCallback, useContext, useEffect, useState } from "react";
import {
  Box,
  Button,
  Card,
  Center,
  Group,
  Loader,
  PasswordInput,
  Stack,
  Text,
  Title,
} from "@mantine/core";
import { IconActivity, IconLock } from "@tabler/icons-react";
import { registerUnauthorizedHandler } from "../lib/apiFetch";

const STORAGE_KEY = "nagare_api_key";

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

interface AuthContextValue {
  apiKey: string | null;
  clearApiKey: () => void;
}

const AuthContext = createContext<AuthContextValue>({
  apiKey: null,
  clearApiKey: () => {},
});

export function useAuthContext() {
  return useContext(AuthContext);
}

// ---------------------------------------------------------------------------
// Provider + Gate
// ---------------------------------------------------------------------------

interface AuthProviderProps {
  children: React.ReactNode;
}

/**
 * AuthProvider wraps the app and gates access behind an API-key prompt.
 *
 * Behaviour:
 * - If no key is stored we show a lock screen.
 * - On submit we probe GET /api/stats with the candidate key.
 * - 200 → store & proceed.  401 → show error.  Other errors / server with no
 *   auth → treat as open (200 with no key also passes through).
 */
export function AuthProvider({ children }: AuthProviderProps) {
  const [apiKey, setApiKeyState] = useState<string | null>(null);
  const [isLoaded, setIsLoaded] = useState(false);

  // Load stored key on mount.
  useEffect(() => {
    const stored = localStorage.getItem(STORAGE_KEY);
    setApiKeyState(stored || null);
    setIsLoaded(true);
  }, []);

  const clearApiKey = useCallback(() => {
    localStorage.removeItem(STORAGE_KEY);
    setApiKeyState(null);
  }, []);

  // Wire the global 401 handler so any apiFetch 401 clears the key.
  useEffect(() => {
    registerUnauthorizedHandler(clearApiKey);
  }, [clearApiKey]);

  const persistKey = useCallback((key: string) => {
    localStorage.setItem(STORAGE_KEY, key);
    setApiKeyState(key);
  }, []);

  if (!isLoaded) {
    return (
      <Center h="100vh">
        <Loader color="cyan" />
      </Center>
    );
  }

  // If there's no stored key, check whether the server requires one.
  // We render the gate only when the server actually responds 401 without a key.
  return (
    <AuthContext.Provider value={{ apiKey, clearApiKey }}>
      <AuthGate apiKey={apiKey} onAuthenticated={persistKey} onClear={clearApiKey}>
        {children}
      </AuthGate>
    </AuthContext.Provider>
  );
}

// ---------------------------------------------------------------------------
// Gate component
// ---------------------------------------------------------------------------

interface AuthGateProps {
  apiKey: string | null;
  onAuthenticated: (key: string) => void;
  onClear: () => void;
  children: React.ReactNode;
}

function AuthGate({ apiKey, onAuthenticated, onClear, children }: AuthGateProps) {
  const [checking, setChecking] = useState(true);
  const [needsAuth, setNeedsAuth] = useState(false);

  // Probe the API to determine if auth is required.
  useEffect(() => {
    let cancelled = false;

    async function probe() {
      setChecking(true);
      try {
        const headers: HeadersInit = {};
        if (apiKey) headers["Authorization"] = `Bearer ${apiKey}`;
        const res = await fetch("/api/stats", { headers });

        if (cancelled) return;

        if (res.status === 401) {
          // Server requires a key — show gate.
          setNeedsAuth(true);
          if (apiKey) {
            // Stored key is stale — clear it.
            onClear();
          }
        } else {
          // 200 or any other status — server is reachable (open or authed).
          setNeedsAuth(false);
        }
      } catch {
        // Network error — still render the app; let individual pages handle it.
        if (!cancelled) setNeedsAuth(false);
      } finally {
        if (!cancelled) setChecking(false);
      }
    }

    probe();
    return () => {
      cancelled = true;
    };
    // Re-probe when apiKey changes (e.g. after successful login or logout).
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [apiKey]);

  if (checking) {
    return (
      <Center h="100vh">
        <Loader color="cyan" />
      </Center>
    );
  }

  if (needsAuth) {
    return <LoginScreen onAuthenticated={onAuthenticated} />;
  }

  return <>{children}</>;
}

// ---------------------------------------------------------------------------
// Login screen
// ---------------------------------------------------------------------------

interface LoginScreenProps {
  onAuthenticated: (key: string) => void;
}

function LoginScreen({ onAuthenticated }: LoginScreenProps) {
  const [value, setValue] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!value.trim()) {
      setError("API key is required.");
      return;
    }
    setLoading(true);
    setError(null);

    try {
      const res = await fetch("/api/stats", {
        headers: { Authorization: `Bearer ${value.trim()}` },
      });

      if (res.ok) {
        onAuthenticated(value.trim());
      } else if (res.status === 401) {
        setError("Invalid API key. Please try again.");
      } else {
        setError(`Unexpected response: ${res.status}`);
      }
    } catch {
      setError("Could not reach the server. Check your connection.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <Center h="100vh" bg="var(--mantine-color-body)">
      <Box w={420} px="md">
        <Stack align="center" mb="xl" gap="xs">
          <Group gap="xs">
            <IconActivity size={32} color="var(--mantine-color-cyan-filled)" />
            <Title order={2} fw={700} c="cyan">
              Nagare
            </Title>
          </Group>
          <Text c="dimmed" size="sm" ta="center">
            Enter your API key to access the dashboard.
          </Text>
        </Stack>

        <Card withBorder shadow="sm" padding="xl" radius="md">
          <form onSubmit={handleSubmit}>
            <Stack gap="md">
              <PasswordInput
                label="API Key"
                placeholder="Enter your API key"
                leftSection={<IconLock size={16} />}
                value={value}
                onChange={(e) => setValue(e.currentTarget.value)}
                error={error}
                autoFocus
                data-autofocus
              />
              <Button type="submit" fullWidth loading={loading} color="cyan" mt="xs">
                Connect
              </Button>
            </Stack>
          </form>
        </Card>
      </Box>
    </Center>
  );
}
