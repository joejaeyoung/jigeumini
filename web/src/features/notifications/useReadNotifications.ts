import { useEffect, useState } from 'react';
import { z } from 'zod';
export const readKey = 'jigeumini.read-notifications.v1';
const readSchema = z.record(z.uuid(), z.number().finite().nonnegative());
export function parseRead(raw: string | null, now = Date.now()): Record<string, number> {
  try {
    const result = readSchema.safeParse(JSON.parse(raw ?? '{}'));
    if (!result.success) return {};
    return Object.fromEntries(Object.entries(result.data).filter(([, at]) => at <= now && at >= now - 7 * 86400000).sort((a, b) => b[1] - a[1]).slice(0, 2000));
  } catch { return {}; }
}
export function useReadNotifications() {
  const [read, setRead] = useState<Record<string, number>>(() => { try { return parseRead(localStorage.getItem(readKey)); } catch { return {}; } });
  const [storageError, setError] = useState(false);
  useEffect(() => {
    const sync = (event: StorageEvent) => { if (event.key === readKey || event.key === null) setRead(parseRead(event.newValue)); };
    window.addEventListener('storage', sync); return () => window.removeEventListener('storage', sync);
  }, []);
  function markRead(id: string) {
    if (!z.uuid().safeParse(id).success) return;
    let current = read;
    try { current = { ...read, ...parseRead(localStorage.getItem(readKey)) }; } catch { /* 현재 화면의 읽음 상태를 유지한다. */ }
    const next = parseRead(JSON.stringify({ ...current, [id]: Date.now() }));
    setRead(next);
    try { localStorage.setItem(readKey, JSON.stringify(next)); setError(false); } catch { setError(true); }
  }
  return { read, markRead, storageError };
}
