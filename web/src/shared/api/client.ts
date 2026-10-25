import { z } from 'zod';

const timestamp = z.iso.datetime({ offset: true });
const count = z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER);
export const channelIdSchema = z.string().regex(/^[a-f0-9]{32}$/);
export const bucketSchema = z.object({ channelId: channelIdSchema, bucketStart: timestamp, chatCount: count });
export const bucketsSchema = z.object({
  asOf: timestamp, windowSeconds: z.number().int().min(10).max(3600), bucketSeconds: z.literal(10),
  includesOpenBucket: z.boolean(), coverageMeasured: z.boolean(), buckets: z.array(bucketSchema),
});
export const statusSchema = z.object({
  asOf: timestamp, scope: z.literal('COLLECTED_CHANNELS_ONLY'), coverageMeasured: z.boolean(),
  consumer: z.object({ running: z.boolean(), state: z.string(), assignedPartitions: count }),
});
export type Bucket = z.infer<typeof bucketSchema>;
export type BucketsResponse = z.infer<typeof bucketsSchema>;
export type Snapshot = { status: z.infer<typeof statusSchema>; counts: BucketsResponse };

export class ApiError extends Error {
  constructor(public readonly reason: 'network' | 'response' | 'contract') { super(reason); }
}
async function read<T>(url: string, schema: z.ZodType<T>, signal: AbortSignal): Promise<T> {
  try {
    const response = await fetch(url, { signal: AbortSignal.any([signal, AbortSignal.timeout(8000)]), headers: { Accept: 'application/json' } });
    if (!response.ok) throw new ApiError('response');
    const parsed = schema.safeParse(await response.json());
    if (!parsed.success) throw new ApiError('contract');
    return parsed.data;
  } catch (error) {
    if (signal.aborted) throw error;
    throw error instanceof ApiError ? error : new ApiError('network');
  }
}
export async function fetchBuckets(seconds: number, signal: AbortSignal, channelId?: string): Promise<BucketsResponse> {
  if (!Number.isInteger(seconds) || seconds < 10 || seconds > 3600 || seconds % 10) throw new ApiError('contract');
  const params = new URLSearchParams({ seconds: String(seconds) });
  if (channelId) params.set('channelId', channelIdSchema.parse(channelId));
  const result = await read(`/api/buckets?${params}`, bucketsSchema, signal);
  if (result.windowSeconds !== seconds || (channelId && result.buckets.some(row => row.channelId !== channelId))) throw new ApiError('contract');
  return result;
}
export async function fetchSnapshot(seconds: number, signal: AbortSignal): Promise<Snapshot> {
  const group = new AbortController();
  const requestSignal = AbortSignal.any([signal, group.signal]);
  try {
    const [status, counts] = await Promise.all([read('/api/status', statusSchema, requestSignal), fetchBuckets(seconds, requestSignal)]);
    return { status, counts };
  } catch (error) {
    group.abort(); // 한 요청이 실패하면 같은 조회의 나머지 요청도 끝낸다.
    throw error;
  }
}

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
