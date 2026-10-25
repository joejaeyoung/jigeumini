import { z } from 'zod';
import { channelIdSchema, count, timestamp } from './fields';
import { ApiError, read } from './transport';

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

