import { useEffect, useRef } from "react";

/**
 * useVisibilityPoll runs `fn` immediately and then on a `intervalMs` interval,
 * but pauses automatically while the browser tab is hidden (document.hidden).
 * This prevents background tabs from hammering the API during active runs.
 */
export function useVisibilityPoll(fn: () => void, intervalMs: number, deps: unknown[] = []) {
  // Stable ref so the interval callback always calls the latest version of fn
  // without needing to be recreated (which would reset the timer).
  const fnRef = useRef(fn);
  useEffect(() => {
    fnRef.current = fn;
  });

  useEffect(() => {
    fnRef.current();

    const tick = () => {
      if (!document.hidden) fnRef.current();
    };

    const id = setInterval(tick, intervalMs);
    return () => clearInterval(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [intervalMs, ...deps]);
}
