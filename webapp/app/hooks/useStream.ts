import { useEffect, useRef } from "react";

export function useStream<T>(
  streamFn: (signal: AbortSignal) => AsyncIterable<T>,
  onMessage: (msg: T) => void,
  deps: unknown[],
) {
  const onMessageRef = useRef(onMessage);
  onMessageRef.current = onMessage;

  useEffect(() => {
    const abort = new AbortController();

    (async () => {
      try {
        for await (const msg of streamFn(abort.signal)) {
          onMessageRef.current(msg);
        }
      } catch (err) {
        if (!abort.signal.aborted) {
          console.error("Stream error:", err);
        }
      }
    })();

    return () => abort.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);
}
