import { afterEach, expect, it, vi } from 'vitest';
import { readFileSync } from 'node:fs';
import { fetchTimeline, observationSchema, type Observation } from './observations';
const id = 'a'.repeat(32), end = Date.parse('2026-10-25T12:00:00Z'), start = end - 3600000;
const iso = (millis: number) => new Date(millis).toISOString();
function sample(index: number, offset: number, count: number | null = 10): Observation {
  return { schemaVersion: 1, eventId: `00000000-0000-4000-8000-${String(index).padStart(12, '0')}`, runId: 'test-v1', channelId: id,
    observedAt: iso(start + offset * 1000), ttlSeconds: 120, viewerCount: count, liveStatus: 'LIVE', quality: count === null ? 'MISSING_VIEWER_COUNT' : 'OBSERVED',
    sourceBroadcastId: '123', broadcastIdentity: 'SOURCE_PROVIDED', categoryId: null, categoryName: null };
}
afterEach(() => vi.unstubAllGlobals());
it('실제 서버 직렬화와 공유하는 0명·null 관측 JSON을 웹 계약으로 읽는다', async () => {
  const payload = readFileSync(new URL('../../../../server/src/test/resources/observation-timeline.json', import.meta.url), 'utf8');
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(payload)));
  const data = await fetchTimeline(id, 3600, new AbortController().signal);
  expect(data.observations.map(sample => sample.viewerCount)).toEqual([0, null]);
  expect(data.chat).toHaveLength(60);
});
it('시청자 0명과 미관측을 구분하고 잘못된 상태와 숫자 변환을 거부한다', () => {
  expect(observationSchema.safeParse(sample(1, 0, 0)).success).toBe(true);
  expect(observationSchema.safeParse(sample(1, 0, null)).success).toBe(true);
  expect(observationSchema.safeParse({ ...sample(1, 0), viewerCount: '10' }).success).toBe(false);
  expect(observationSchema.safeParse({ ...sample(1, 0), liveStatus: 'OFFLINE' }).success).toBe(false);
  expect(observationSchema.safeParse({ ...sample(1, 0), broadcastIdentity: 'UNAVAILABLE' }).success).toBe(false);
});
it('서버의 완료 창과 관측 순서·채널·중복을 검증한다', async () => {
  const payload = { channelId: id, asOf: iso(end + 1234), from: iso(start), to: iso(end), truncated: false, observations: [sample(1, 1), sample(2, 61)],
    chat: Array.from({ length: 60 }, (_, i) => ({ from: iso(start + i * 60000), to: iso(start + (i + 1) * 60000), count: 0, referenceCount: 0, coverage: 1, status: 'HEALTHY', reasons: [] })) };
  const fetcher = vi.fn(); vi.stubGlobal('fetch', fetcher);
  fetcher.mockResolvedValue(new Response(JSON.stringify(payload))); await expect(fetchTimeline(id, 3600, new AbortController().signal)).resolves.toMatchObject({ to: iso(end) });
  for (const invalid of [{ ...payload, chat: payload.chat.slice(1) }, { ...payload, observations: [...payload.observations].reverse() },
    { ...payload, observations: [sample(1, 3600)] }, { ...payload, observations: [sample(1, 1), sample(1, 1)] },
    { ...payload, observations: [{ ...sample(1, 1), channelId: 'b'.repeat(32) }] }]) {
    fetcher.mockResolvedValue(new Response(JSON.stringify(invalid))); await expect(fetchTimeline(id, 3600, new AbortController().signal)).rejects.toMatchObject({ reason: 'contract' });
  }
});
