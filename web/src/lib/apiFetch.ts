const STORAGE_KEY = "nagare_api_key";

type OnUnauthorized = () => void;

let globalOnUnauthorized: OnUnauthorized | null = null;

/** Register a callback invoked on any 401 response (clears the stored key). */
export function registerUnauthorizedHandler(cb: OnUnauthorized) {
  globalOnUnauthorized = cb;
}

/**
 * Drop-in replacement for `fetch` that:
 * 1. Attaches `Authorization: Bearer <key>` when a key is stored.
 * 2. Fires the registered 401 handler when the server rejects the key.
 */
export async function apiFetch(
  input: RequestInfo | URL,
  init: RequestInit = {}
): Promise<Response> {
  const key = localStorage.getItem(STORAGE_KEY);

  const headers = new Headers(init.headers);
  if (key) {
    headers.set("Authorization", `Bearer ${key}`);
  }

  const res = await fetch(input, { ...init, headers });

  if (res.status === 401 && globalOnUnauthorized) {
    globalOnUnauthorized();
  }

  return res;
}
