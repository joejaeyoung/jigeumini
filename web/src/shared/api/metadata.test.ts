import { afterEach, expect, it, vi } from 'vitest';
import { fetchChannel, fetchChannels, metadataSchema, type BroadcastMetadata } from './client';
import { currentMetadata, matchesSearch, withMetadata } from '../../features/broadcasts/model';
const id = 'a'.repeat(32);
const time = Date.parse('2026-10-25T00:00:00Z');
const data: BroadcastMetadata = {
  channelId: id, channelName: '합성 채널', title: '숲속 모험', categoryId: 'game', categoryName: '테스트 게임',
  thumbnailUrl: 'https://livecloud.pstatic.net/480.jpg', liveStatus: 'LIVE', freshness: 'FRESH', reason: 'NONE',
  checkedAt: new Date(time).toISOString(), expiresAt: new Date(time + 60000).toISOString(),
};
afterEach(() => { vi.unstubAllGlobals(); vi.useRealTimers(); });
it('방송 상태·썸네일 출처·응답 ID를 검사한다', async () => {
  expect(metadataSchema.safeParse(data).success).toBe(true);
  expect(metadataSchema.safeParse({ ...data, thumbnailUrl: 'https://livecloud-thumb.akamaized.net/480.jpg' }).success).toBe(true);
  expect(metadataSchema.safeParse({ ...data, thumbnailUrl: 'https://other.akamaized.net/480.jpg' }).success).toBe(false);
  expect(metadataSchema.safeParse({ ...data, liveStatus: 'RESTRICTED' }).success).toBe(false);
  expect(metadataSchema.safeParse({ ...data, thumbnailUrl: 'https://pstatic.net.evil.test/a.jpg' }).success).toBe(false);
  expect(metadataSchema.safeParse({ ...data, freshness: 'STALE' }).success).toBe(false);
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify(data))));
  await expect(fetchChannel('b'.repeat(32), new AbortController().signal)).rejects.toMatchObject({ reason: 'contract' });
});
it('서버 연결이 끊겨도 만료 후 LIVE·이미지, 5분 후 공개 정보까지 내린다', () => {
  expect(currentMetadata(data, time + 59999)?.liveStatus).toBe('LIVE');
  expect(currentMetadata(data, time + 60000)).toMatchObject({ liveStatus: 'UNKNOWN', thumbnailUrl: null, freshness: 'STALE' });
  expect(currentMetadata(data, time + 300000)).toBeUndefined();
});
it('최근 집계가 없는 실제 채널을 추가하되 0건을 만들지 않고 제목·채널명·카테고리로 검색한다', () => {
  vi.useFakeTimers(); vi.setSystemTime(time);
  const [channel] = withMetadata([], [data]);
  expect(channel.chatCount).toBeNull();
  for (const query of ['숲속', '합성', '게임', 'aaaa']) expect(matchesSearch(channel, query)).toBe(true);
  expect(matchesSearch(channel, '없는 방송')).toBe(false);
});
it('20개 단위로 관심 채널을 조회하고 누락·중복 응답을 거부한다', async () => {
  const ids = Array.from({ length: 21 }, (_, i) => i.toString(16).padStart(32, '0'));
  const fetcher = vi.fn(async (url: string) => {
    const requested = new URL(url, 'https://test.local').searchParams.get('ids')!.split(',');
    return new Response(JSON.stringify({ asOf: data.checkedAt, scope: 'REQUESTED_CHANNELS', channels: requested.map(channelId => ({ ...data, channelId })) }));
  });
  vi.stubGlobal('fetch', fetcher);
  expect(await fetchChannels(new AbortController().signal, ids)).toHaveLength(21);
  expect(fetcher).toHaveBeenCalledTimes(2);
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ asOf: data.checkedAt, scope: 'REQUESTED_CHANNELS', channels: [] }))));
  await expect(fetchChannels(new AbortController().signal, [id])).rejects.toMatchObject({ reason: 'contract' });
});
