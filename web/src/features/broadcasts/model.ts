import { rankingStale, type Rankings, type RankingEntry } from '../../shared/api/quality';
import type { Bucket } from '../../shared/api/buckets';
import type { BroadcastMetadata } from '../../shared/api/metadata';
export type ChannelSummary = { id: string; chatCount: number | null; lastBucket: string | null; metadata?: BroadcastMetadata; quality?: RankingEntry; stale?: boolean };
export function summarize(buckets: Bucket[]): ChannelSummary[] {
  const result = new Map<string, ChannelSummary>();
  for (const bucket of buckets) {
    const previous = result.get(bucket.channelId);
    const total = (previous?.chatCount ?? 0) + bucket.chatCount;
    if (!Number.isSafeInteger(total)) throw new Error('집계 범위 초과');
    result.set(bucket.channelId, {
      id: bucket.channelId, chatCount: total,
      lastBucket: !previous?.lastBucket || bucket.bucketStart > previous.lastBucket ? bucket.bucketStart : previous.lastBucket,
    });
  }
  return [...result.values()].sort((a, b) => (b.chatCount ?? 0) - (a.chatCount ?? 0) || a.id.localeCompare(b.id));
}
export function withSavedChannels(channels: ChannelSummary[], saved: string[]): ChannelSummary[] {
  const current = new Map(channels.map(channel => [channel.id, channel]));
  return saved.map(id => current.get(id) ?? { id, chatCount: null, lastBucket: null });
}
export const channelName = (id: string) => `수집 채널 ${id.slice(0, 8)}`;
export const formatCount = (count: number) => count.toLocaleString('ko-KR');
export const formatTime = (time: string) => new Date(time).toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit', second: '2-digit' });

// 서버 연결이 끊겨도 마지막 LIVE 표시나 이미지가 계속 남지 않도록 브라우저에서도 만료시킨다.
export function currentMetadata(data: BroadcastMetadata | undefined, now = Date.now()): BroadcastMetadata | undefined {
  if (!data) return undefined;
  if (data.checkedAt && now >= Date.parse(data.checkedAt) + 300_000) return undefined;
  if (data.expiresAt && now >= Date.parse(data.expiresAt)) return {
    ...data, liveStatus: 'UNKNOWN', thumbnailUrl: null,
    freshness: data.channelName ? 'STALE' : 'UNAVAILABLE', reason: 'PENDING',
  };
  return data;
}
export function withMetadata(channels: ChannelSummary[], metadata: BroadcastMetadata[]): ChannelSummary[] {
  const merged = new Map(channels.map(channel => [channel.id, channel]));
  for (const data of metadata) {
    const channel = merged.get(data.channelId) ?? { id: data.channelId, chatCount: null, lastBucket: null };
    merged.set(data.channelId, { ...channel, metadata: currentMetadata(data) });
  }
  return [...merged.values()];
}
export function matchesSearch(channel: ChannelSummary, query: string): boolean {
  const meta = channel.metadata;
  return [channel.id, meta?.channelName, meta?.title, meta?.categoryName].some(value => value?.toLocaleLowerCase().includes(query));
}
export function metadataLabel(meta?: BroadcastMetadata): string {
  if (!meta) return '방송 정보 확인 전';
  if (meta.freshness === 'STALE') return '이전 방송 정보 · 상태 재확인 중';
  switch (meta.liveStatus) {
    case 'LIVE': return '방송 중';
    case 'OFFLINE': return '방송 종료';
    case 'RESTRICTED': return '성인·접근 제한';
    case 'NOT_FOUND': return '채널을 찾을 수 없음';
    default: return meta.reason === 'PENDING' ? '방송 정보 확인 중' : '방송 정보 확인 불가';
  }
}

export function rankedChannels(data: Rankings): ChannelSummary[] {
  const stale = rankingStale(data);
  return [...data.entries, ...data.excludedChannels].map(entry => ({ id: entry.channelId,
    chatCount: stale ? null : entry.count, lastBucket: data.evaluatedAt, quality: entry, stale }));
}
