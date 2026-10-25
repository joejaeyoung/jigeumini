import { z } from 'zod';
import { channelIdSchema, timestamp } from './fields';
import { ApiError, read } from './transport';

const thumbnailUrl = z.url().refine(value => {
  const url = new URL(value);
  return url.protocol === 'https:' && !url.username && !url.password && !url.port
    && (url.hostname === 'livecloud-thumb.akamaized.net' || url.hostname.endsWith('.pstatic.net') || url.hostname.endsWith('.naver.net'));
});
export const metadataSchema = z.object({
  channelId: channelIdSchema, channelName: z.string().max(120).nullable(), title: z.string().max(300).nullable(),
  categoryId: z.string().max(120).nullable(), categoryName: z.string().max(120).nullable(), thumbnailUrl: thumbnailUrl.nullable(),
  liveStatus: z.enum(['LIVE', 'OFFLINE', 'UNKNOWN', 'RESTRICTED', 'NOT_FOUND']),
  freshness: z.enum(['FRESH', 'STALE', 'UNAVAILABLE']),
  reason: z.enum(['NONE', 'PENDING', 'RESTRICTED', 'NOT_FOUND', 'RATE_LIMITED', 'UPSTREAM_ERROR', 'INVALID_RESPONSE', 'DISABLED']),
  checkedAt: timestamp.nullable(), expiresAt: timestamp.nullable(),
}).superRefine((value, context) => {
  if ((value.freshness !== 'UNAVAILABLE' && (!value.checkedAt || !value.expiresAt))
    || (value.liveStatus === 'LIVE' && value.freshness !== 'FRESH')
    || (['RESTRICTED', 'NOT_FOUND'].includes(value.liveStatus) && [value.channelName, value.title, value.categoryId, value.categoryName, value.thumbnailUrl].some(Boolean))) {
    context.addIssue({ code: 'custom', message: '방송 상태와 공개 정보가 일치하지 않음' });
  }
});
export type BroadcastMetadata = z.infer<typeof metadataSchema>;
const channelsSchema = z.object({ asOf: timestamp, scope: z.enum(['COLLECTED_CHANNELS_ONLY', 'REQUESTED_CHANNELS']), channels: z.array(metadataSchema).max(20) });
export async function fetchChannels(signal: AbortSignal, ids?: string[]): Promise<BroadcastMetadata[]> {
  if (!ids) return (await read('/api/channels', channelsSchema, signal)).channels;
  const unique = [...new Set(ids.map(id => channelIdSchema.parse(id)))];
  const result: BroadcastMetadata[] = [];
  for (let i = 0; i < unique.length; i += 20) {
    const batch = unique.slice(i, i + 20);
    const data = await read(`/api/channels?${new URLSearchParams({ ids: batch.join(',') })}`, channelsSchema, signal);
    if (data.channels.length !== batch.length || new Set(data.channels.map(channel => channel.channelId)).size !== batch.length || data.channels.some(channel => !batch.includes(channel.channelId))) throw new ApiError('contract');
    result.push(...data.channels);
  }
  return result;
}
export async function fetchChannel(id: string, signal: AbortSignal): Promise<BroadcastMetadata> {
  const data = await read(`/api/channels/${channelIdSchema.parse(id)}`, metadataSchema, signal);
  if (data.channelId !== id) throw new ApiError('contract');
  return data;
}
