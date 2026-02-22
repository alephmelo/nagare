import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { useAuth } from "@/hooks/useAuth";

const STORAGE_KEY = "nagare_api_key";

describe("useAuth", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    localStorage.clear();
  });

  it("loads apiKey from localStorage and sets isLoaded true after mount", async () => {
    localStorage.setItem(STORAGE_KEY, "stored-key");

    const { result } = renderHook(() => useAuth());

    // Wait for the effect to flush
    await act(async () => {});
    expect(result.current.isLoaded).toBe(true);
    expect(result.current.apiKey).toBe("stored-key");
  });

  it("returns null apiKey when nothing is stored", async () => {
    const { result } = renderHook(() => useAuth());

    await act(async () => {});

    expect(result.current.apiKey).toBeNull();
    expect(result.current.isLoaded).toBe(true);
  });

  it("setApiKey persists the key to localStorage and updates state", async () => {
    const { result } = renderHook(() => useAuth());
    await act(async () => {});

    act(() => {
      result.current.setApiKey("new-key");
    });

    expect(result.current.apiKey).toBe("new-key");
    expect(localStorage.getItem(STORAGE_KEY)).toBe("new-key");
  });

  it("clearApiKey removes the key from localStorage and sets state to null", async () => {
    localStorage.setItem(STORAGE_KEY, "existing-key");
    const { result } = renderHook(() => useAuth());
    await act(async () => {});

    act(() => {
      result.current.clearApiKey();
    });

    expect(result.current.apiKey).toBeNull();
    expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
  });
});
