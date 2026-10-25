import { afterEach, expect, it, vi } from 'vitest';
import { fetchTimeline, observationSchema, viewerSegments, type Observation } from './observations';
const id = 'a'.repeat(32), end = Date.parse('2026-10-25T12:00:00Z'), start = end - 3600000;
const iso = (millis: number) => new Date(millis).toISOString();
function sample(index: number, offset: number, count: number | null = 10): Observation {
  return { schemaVersion: 1, eventId: `00000000-0000-4000-8000-${String(index).padStart(12, '0')}`, runId: 'test-v1', channelId: id,
    observedAt: iso(start + offset * 1000), ttlSeconds: 120, viewerCount: count, liveStatus: 'LIVE', quality: count === null ? 'MISSING_VIEWER_COUNT' : 'OBSERVED',
    sourceBroadcastId: '123', broadcastIdentity: 'SOURCE_PROVIDED', categoryId: null, categoryName: null };
}
afterEach(() => vi.unstubAllGlobals());
it('시청자 0명과 미관측을 구분하고 잘못된 상태와 숫자 변환을 거부한다', () => {
  expect(observationSchema.safeParse(sample(1, 0, 0)).success).toBe(true);
  expect(observationSchema.safeParse(sample(1, 0, null)).success).toBe(true);
  expect(observationSchema.safeParse({ ...sample(1, 0), viewerCount: '10' }).success).toBe(false);
  expect(observationSchema.safeParse({ ...sample(1, 0), liveStatus: 'OFFLINE' }).success).toBe(false);
  expect(observationSchema.safeParse({ ...sample(1, 0), broadcastIdentity: 'UNAVAILABLE' }).success).toBe(false);
});
it('실패·TTL 초과·방송 전환에 선을 끊고 관측 시각을 옮기거나 연장하지 않는다', () => {
  const points = [sample(1, 7, 0), sample(2, 67), sample(3, 127, null), sample(4, 187), sample(5, 307), sample(6, 428), { ...sample(7, 488), sourceBroadcastId: '456' }];
  const segments = viewerSegments(points);
  expect(segments.map(segment => segment.map(point => point.eventId))).toEqual([[points[0].eventId, points[1].eventId], [points[3].eventId, points[4].eventId], [points[5].eventId], [points[6].eventId]]);
  expect(segments[0][0].observedAt).toBe(iso(start + 7000));
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
