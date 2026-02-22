import "@testing-library/jest-dom";
import { vi } from "vitest";

// Vitest 4's jsdom environment replaces the native Storage-based localStorage
// with a file-backed implementation that may not be fully initialised in CI.
// Stub it with a reliable in-memory implementation so tests can use the full
// Storage API (getItem / setItem / removeItem / clear / key / length).
function createLocalStorageMock() {
  let store: Record<string, string> = {};

  return {
    get length() {
      return Object.keys(store).length;
    },
    key(index: number): string | null {
      return Object.keys(store)[index] ?? null;
    },
    getItem(key: string): string | null {
      return Object.prototype.hasOwnProperty.call(store, key) ? store[key] : null;
    },
    setItem(key: string, value: string): void {
      store[key] = String(value);
    },
    removeItem(key: string): void {
      delete store[key];
    },
    clear(): void {
      store = {};
    },
  };
}

vi.stubGlobal("localStorage", createLocalStorageMock());
