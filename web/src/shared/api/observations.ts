import { z } from 'zod';
import { channelIdSchema, count, timestamp } from './fields';
import { activityPointSchema } from './quality';
import { ApiError, read } from './transport';
import { isMinuteTimeline } from './minuteTimeline';

export const observationSchema = z.object({
  schemaVersion: z.literal(1), eventId: z.uuid(), runId: z.string().regex(/^[a-zA-Z0-9_-]{1,80}$/), channelId: channelIdSchema,
  observedAt: timestamp, ttlSeconds: z.number().int().min(60).max(1800), viewerCount: count.nullable(),
  liveStatus: z.enum(['LIVE', 'OFFLINE', 'UNKNOWN', 'RESTRICTED', 'NOT_FOUND']),
  quality: z.enum(['OBSERVED', 'MISSING_VIEWER_COUNT', 'OFFLINE', 'RESTRICTED', 'NOT_FOUND', 'RATE_LIMITED', 'UPSTREAM_ERROR', 'INVALID_RESPONSE']),
  sourceBroadcastId: z.string().regex(/^[1-9][0-9]{0,18}$/).nullable(), broadcastIdentity: z.enum(['SOURCE_PROVIDED', 'UNAVAILABLE']),
  categoryId: z.string().max(120).nullable(), categoryName: z.string().max(120).nullable(),
}).superRefine((sample, ctx) => {
  const valid = sample.liveStatus === 'LIVE' ? (sample.quality === 'OBSERVED' && sample.viewerCount !== null) || (sample.quality === 'MISSING_VIEWER_COUNT' && sample.viewerCount === null)
    : sample.viewerCount === null && sample.categoryId === null && sample.categoryName === null && (sample.liveStatus === 'UNKNOWN'
      ? ['RATE_LIMITED', 'UPSTREAM_ERROR', 'INVALID_RESPONSE'].includes(sample.quality) : sample.quality === sample.liveStatus);
  if (!valid || (sample.sourceBroadcastId !== null) !== (sample.broadcastIdentity === 'SOURCE_PROVIDED')
    || (!['LIVE', 'OFFLINE'].includes(sample.liveStatus) && sample.sourceBroadcastId !== null)) ctx.addIssue({ code: 'custom', message: '관측 상태와 수치 불일치' });
});
export const timelineSchema = z.object({
  channelId: channelIdSchema, asOf: timestamp, from: timestamp, to: timestamp,
  chat: z.array(activityPointSchema).max(60), observations: z.array(observationSchema).max(1000), truncated: z.boolean(),
});
export type Observation = z.infer<typeof observationSchema>;
export type Timeline = z.infer<typeof timelineSchema>;
export async function fetchTimeline(id: string, seconds: number, signal: AbortSignal): Promise<Timeline> {
  if (seconds !== 1800 && seconds !== 3600) throw new ApiError('contract');
  const data = await read(`/api/v1/channels/${channelIdSchema.parse(id)}/timeline?seconds=${seconds}`, timelineSchema, signal);
  const from = Date.parse(data.from), to = Date.parse(data.to);
  if (data.channelId !== id || to - from !== seconds * 1000 || to > Date.parse(data.asOf)
    || !isMinuteTimeline(data.chat, from, to)
    || new Set(data.observations.map(point => point.eventId)).size !== data.observations.length
    || data.observations.some((point, i, all) => point.channelId !== id || Date.parse(point.observedAt) < from || Date.parse(point.observedAt) >= to
      || (i > 0 && Date.parse(point.observedAt) < Date.parse(all[i - 1].observedAt)))) throw new ApiError('contract');
  return data;
}
