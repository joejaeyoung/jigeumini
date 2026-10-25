import { useEffect, useState } from 'react';
import { channelIdSchema } from '../../shared/api/fields';
export const storageKey = 'jigeumini.saved-channels.v1';
export function parseSaved(raw: string | null): string[] {
  try {
    const data: unknown = JSON.parse(raw ?? '[]');
    if (!Array.isArray(data)) return [];
    return [...new Set(data.filter((id): id is string => channelIdSchema.safeParse(id).success))];
  } catch { return []; }
}
export function useSavedChannels() {
  const [saved, setSaved] = useState<string[]>(() => {
    try { return parseSaved(localStorage.getItem(storageKey)); } catch { return []; }
  });
  const [storageError, setStorageError] = useState(false);
  useEffect(() => {
    const onStorage = (event: StorageEvent) => {
      if (event.key === storageKey || event.key === null) setSaved(parseSaved(event.newValue));
    };
    window.addEventListener('storage', onStorage);
    return () => window.removeEventListener('storage', onStorage);
  }, []);
  function toggle(id: string) {
    if (!channelIdSchema.safeParse(id).success) return;
    const next = saved.includes(id) ? saved.filter(value => value !== id) : [...saved, id];
    setSaved(next);
    try { localStorage.setItem(storageKey, JSON.stringify(next)); setStorageError(false); }
    catch { setStorageError(true); }
  }
  return { saved, toggle, storageError };
}
