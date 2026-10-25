import { z } from 'zod';
import { ApiError, channelIdSchema, read } from './client';
const time = z.iso.datetime({ offset: true });
const count = z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER);
export const qualityStatus = z.enum(['HEALTHY', 'DISABLED', 'OFFLINE', 'INCOMPLETE', 'WARMING_UP', 'LAGGING', 'UNKNOWN']);
export const rankingEntrySchema = z.object({
  channelId: channelIdSchema, rank: z.number().int().positive().nullable(), count: count.nullable(), referenceCount: count,
  coverage: z.number().min(0).max(1), status: qualityStatus, reasons: z.array(z.string()),
  previousRank: z.number().int().positive().nullable(), previousReason: z.string().nullable(),
}).superRefine((entry, ctx) => {
  const healthy = entry.status === 'HEALTHY';
  if ((healthy && (entry.rank === null || entry.count === null || entry.coverage < .95 || entry.count !== entry.referenceCount || entry.reasons.length > 0))
    || (!healthy && (entry.rank !== null || entry.count !== null))) ctx.addIssue({ code: 'custom', message: '품질과 순위 계약 불일치' });
});
export const rankingsSchema = z.object({
  asOf: time, evaluatedAt: time.nullable(), snapshotId: z.uuid().nullable(), windowSeconds: z.union([z.literal(60), z.literal(300)]),
  algorithmVersion: z.literal('quality-ranking-v1'), status: z.enum(['READY', 'STALE', 'WARMING_UP']),
  entries: z.array(rankingEntrySchema).max(100), excludedChannels: z.array(rankingEntrySchema).max(100),
}).superRefine((data, ctx) => {
  if ((data.entries.length > 0 && (!data.evaluatedAt || !data.snapshotId)) || data.entries.some(e => e.status !== 'HEALTHY') || data.excludedChannels.some(e => e.status === 'HEALTHY')) ctx.addIssue({ code: 'custom', message: '스냅샷 계약 불일치' });
});
export type RankingEntry = z.infer<typeof rankingEntrySchema>;
export type Rankings = z.infer<typeof rankingsSchema>;
export async function fetchRankings(window: number, signal: AbortSignal): Promise<Rankings> {
  if (window !== 60 && window !== 300) throw new ApiError('contract');
  const data = await read(`/api/v1/rankings?window=${window}&limit=20`, rankingsSchema, signal);
  if (data.windowSeconds !== window) throw new ApiError('contract');
  return data;
}
export const spikeSchema = z.object({
  spikeId: z.uuid(), channelId: channelIdSchema, evaluatedAt: time, count, baseline: z.number().nonnegative(), ratio: z.number().finite().nonnegative(),
  algorithmVersion: z.literal('chat-spike-v1'), valid: z.boolean(), qualityWarning: z.string().nullable(),
}).superRefine((data, ctx) => {
  if (data.valid && (data.qualityWarning !== null || data.count < 100 || data.count - data.baseline < 50 || data.ratio < 2 || Math.abs(data.count / Math.max(data.baseline, 10) - data.ratio) > .000001)) ctx.addIssue({ code: 'custom', message: '급상승 판정 계약 불일치' });
});
export const spikesSchema = z.object({ asOf: time, status: z.enum(['READY', 'STALE', 'WARMING_UP']), events: z.array(spikeSchema).max(100), nextCursor: z.string().nullable() });
export type Spikes = z.infer<typeof spikesSchema>;
export async function fetchSpikes(signal: AbortSignal, cursor = ''): Promise<Spikes> {
  return read(`/api/v1/spikes?${new URLSearchParams({ cursor, limit: '20' })}`, spikesSchema, signal);
}
const activitySchema = z.object({ channelId: channelIdSchema, asOf: time, from: time, to: time, points: z.array(z.object({
  from: time, to: time, count: count.nullable(), referenceCount: count, coverage: z.number().min(0).max(1), status: qualityStatus, reasons: z.array(z.string()),
}).refine(point => point.status === 'HEALTHY' ? point.count !== null && point.coverage >= .95 : point.count === null)).max(60) });
export async function fetchActivity(id: string, seconds: number, signal: AbortSignal) {
  if (seconds !== 1800 && seconds !== 3600) throw new ApiError('contract');
  const data = await read(`/api/v1/channels/${channelIdSchema.parse(id)}/activity?seconds=${seconds}`, activitySchema, signal);
  const from = Date.parse(data.from), to = Date.parse(data.to);
  if (data.channelId !== id || from % 60000 !== 0 || to % 60000 !== 0 || to - from !== seconds * 1000
    || data.points.length !== seconds / 60 || data.points.some((point, i) => Date.parse(point.from) !== from + i * 60000 || Date.parse(point.to) !== from + (i + 1) * 60000)) throw new ApiError('contract');
  return data;
}
export function rankingStale(data: Rankings, now = Date.now()) { return data.status === 'STALE' || Boolean(data.evaluatedAt && now >= Date.parse(data.evaluatedAt) + 30000); }
export const qualityLabel: Record<RankingEntry['status'], string> = {
  HEALTHY: '수집 품질 확인', DISABLED: '수집 중지', OFFLINE: '방송 종료', INCOMPLETE: '수집 불완전', WARMING_UP: '집계 준비 중', LAGGING: '반영 지연', UNKNOWN: '수집 상태 미확인',
};
