import { afterEach, expect, it, vi } from 'vitest';
import { fetchActivity, rankingsSchema, rankingStale, spikeSchema, type Rankings } from './quality';
import { rankedChannels } from '../../features/broadcasts/model';
const at = '2026-10-25T12:00:00Z';
const entry = { channelId: 'a'.repeat(32), rank: 1, count: 0, referenceCount: 0, coverage: 1, status: 'HEALTHY', reasons: [], previousRank: null, previousReason: 'NO_PREVIOUS_SNAPSHOT' };
const response = { asOf: at, evaluatedAt: at, snapshotId: '00000000-0000-4000-8000-000000000001', windowSeconds: 60, algorithmVersion: 'quality-ranking-v1', status: 'READY', entries: [entry], excludedChannels: [] };
it('정상 관측된 0건만 순위에 포함하고 품질 불량의 수치를 거부한다', () => {
  expect(rankingsSchema.safeParse(response).success).toBe(true);
  expect(rankingsSchema.safeParse({ ...response, entries: [{ ...entry, coverage: .949 }] }).success).toBe(false);
  expect(rankingsSchema.safeParse({ ...response, entries: [{ ...entry, status: 'INCOMPLETE' }] }).success).toBe(false);
  expect(rankingsSchema.safeParse({ ...response, evaluatedAt: null }).success).toBe(false);
});
it('30초 경계부터 갱신이 중단된 스냅샷을 최신 순위로 취급하지 않는다', () => {
  const data = rankingsSchema.parse(response);
  expect(rankingStale(data, Date.parse(at) + 29999)).toBe(false);
  expect(rankingStale(data, Date.parse(at) + 30000)).toBe(true);
  expect(rankedChannels({ ...data, status: 'STALE' } as Rankings)[0].chatCount).toBeNull();
});
it('급상승 임계값·비율이 맞지 않거나 품질 경고가 있는 유효 리본을 거부한다', () => {
  const spike = { spikeId: response.snapshotId, channelId: entry.channelId, evaluatedAt: at, count: 100, baseline: 50, ratio: 2, algorithmVersion: 'chat-spike-v1', valid: true, qualityWarning: null };
  expect(spikeSchema.safeParse(spike).success).toBe(true);
  expect(spikeSchema.safeParse({ ...spike, baseline: 50.2 }).success).toBe(false);
  expect(spikeSchema.safeParse({ ...spike, count: 99 }).success).toBe(false);
  expect(spikeSchema.safeParse({ ...spike, ratio: 3 }).success).toBe(false);
  expect(spikeSchema.safeParse({ ...spike, qualityWarning: 'COLLECTION_REVISED' }).success).toBe(false);
  expect(spikeSchema.safeParse({ ...spike, valid: false, qualityWarning: 'COLLECTION_REVISED' }).success).toBe(true);
});

afterEach(() => { vi.unstubAllGlobals(); vi.useRealTimers(); });
it('상세 조회는 브라우저 시계 대신 서버 범위를 검증하고 누락 point를 거부한다', async () => {
  vi.useFakeTimers(); vi.setSystemTime(new Date('2040-01-01T00:00:00Z'));
  const end = Date.parse(at), start = end - 3600000;
  const payload = { channelId: entry.channelId, asOf: at, from: new Date(start).toISOString(), to: at,
    points: Array.from({ length: 60 }, (_, i) => ({ from: new Date(start + i * 60000).toISOString(), to: new Date(start + (i + 1) * 60000).toISOString(), count: 0, referenceCount: 0, coverage: 1, status: 'HEALTHY', reasons: [] })) };
  const fetcher = vi.fn().mockResolvedValue(new Response(JSON.stringify(payload), { status: 200 }));
  vi.stubGlobal('fetch', fetcher);
  await expect(fetchActivity(entry.channelId, 3600, new AbortController().signal)).resolves.toMatchObject({ to: at });
  expect(fetcher.mock.calls[0][0]).toBe(`/api/v1/channels/${entry.channelId}/activity?seconds=3600`);
  fetcher.mockResolvedValue(new Response(JSON.stringify({ ...payload, points: payload.points.slice(1) }), { status: 200 }));
  await expect(fetchActivity(entry.channelId, 3600, new AbortController().signal)).rejects.toMatchObject({ reason: 'contract' });
});
