import type { Bucket } from '../../shared/api/client';
export type ChannelSummary = { id: string; chatCount: number | null; lastBucket: string | null };
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
