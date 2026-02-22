import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { apiFetch, registerUnauthorizedHandler } from "@/lib/apiFetch";

const STORAGE_KEY = "nagare_api_key";

describe("apiFetch", () => {
  beforeEach(() => {
    localStorage.clear();
    // Reset the global unauthorized handler between tests
    registerUnauthorizedHandler(() => {});
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(null, { status: 200 }));
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("makes a request without Authorization header when no key is stored", async () => {
    await apiFetch("/api/dags");

    const [, init] = (fetch as ReturnType<typeof vi.fn>).mock.calls[0];
    const headers = init.headers as Headers;
    expect(headers.has("Authorization")).toBe(false);
  });

  it("attaches Bearer token when an API key is stored", async () => {
    localStorage.setItem(STORAGE_KEY, "test-key-123");

    await apiFetch("/api/dags");

    const [, init] = (fetch as ReturnType<typeof vi.fn>).mock.calls[0];
    const headers = init.headers as Headers;
    expect(headers.get("Authorization")).toBe("Bearer test-key-123");
  });

  it("passes through the URL and additional init options", async () => {
    localStorage.setItem(STORAGE_KEY, "key");

    await apiFetch("/api/dags", { method: "POST", body: '{"foo":"bar"}' });

    const [url, init] = (fetch as ReturnType<typeof vi.fn>).mock.calls[0];
    expect(url).toBe("/api/dags");
    expect(init.method).toBe("POST");
    expect(init.body).toBe('{"foo":"bar"}');
  });

  it("calls the registered unauthorized handler on 401", async () => {
    (fetch as ReturnType<typeof vi.fn>).mockResolvedValue(new Response(null, { status: 401 }));
    const handler = vi.fn();
    registerUnauthorizedHandler(handler);

    await apiFetch("/api/protected");

    expect(handler).toHaveBeenCalledOnce();
  });

  it("does not call the unauthorized handler on non-401 responses", async () => {
    (fetch as ReturnType<typeof vi.fn>).mockResolvedValue(new Response(null, { status: 403 }));
    const handler = vi.fn();
    registerUnauthorizedHandler(handler);

    await apiFetch("/api/protected");

    expect(handler).not.toHaveBeenCalled();
  });

  it("returns the raw Response", async () => {
    const mockResponse = new Response('{"ok":true}', { status: 200 });
    (fetch as ReturnType<typeof vi.fn>).mockResolvedValue(mockResponse);

    const res = await apiFetch("/api/dags");

    expect(res).toBe(mockResponse);
  });
});
