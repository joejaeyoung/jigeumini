import { useCallback, useEffect, useRef, useState } from 'react';

type Result<T> = { key: string; data: T | null; error: boolean; loading: boolean };
export function usePolling<T>(key: string, load: (signal: AbortSignal) => Promise<T>, interval = 5000) {
  const loader = useRef(load);
  loader.current = load;
  const [attempt, setAttempt] = useState(0);
  const [result, setResult] = useState<Result<T>>({ key, data: null, error: false, loading: true });
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    setResult(previous => ({ key, data: previous.key === key ? previous.data : null, error: false, loading: true }));
    async function refresh() {
      try {
        const data = await loader.current(controller.signal);
        if (!controller.signal.aborted) setResult({ key, data, error: false, loading: false });
      } catch {
        if (!controller.signal.aborted) setResult(previous => ({ key, data: previous.key === key ? previous.data : null, error: true, loading: false }));
      } finally {
        if (!controller.signal.aborted) timer = setTimeout(refresh, interval);
      }
    }
    void refresh();
    return () => { controller.abort(); clearTimeout(timer); };
  }, [key, attempt, interval]);
  const retry = useCallback(() => setAttempt(value => value + 1), []);
  // 주소가 바뀐 렌더에서 이전 기간의 값을 노출하지 않는다.
  return { ...(result.key === key ? result : { key, data: null, error: false, loading: true }), retry };
}
