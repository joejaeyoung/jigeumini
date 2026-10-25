import { z } from 'zod';
import { channelIdSchema, count, timestamp } from './fields';
import { ApiError, read } from './transport';
import { isMinuteTimeline } from './minuteTimeline';

export const categoryKey = z.string().regex(/^[a-f0-9]{64}$|^unknown$/);
const channel = z.object({ channelId: channelIdSchema, count: count.nullable(), referenceCount: count, quality: z.enum(['HEALTHY', 'INCOMPLETE']) });
const entry = z.object({ categoryKey, name: z.string().min(1).max(120), count: count.nullable(), referenceCount: count,
  observedChannels: count.max(20), healthyChannels: count.max(20), share: z.number().min(0).max(1).nullable(), channels: z.array(channel).max(20) });
const status = z.enum(['READY', 'LAGGING']);
export const categoriesSchema = z.object({ asOf: timestamp, from: timestamp, to: timestamp, scope: z.literal('COLLECTED_CHANNELS_ONLY'),
  status, revision: count, unknownReferenceCount: count, truncated: z.boolean(), categories: z.array(entry).max(1020) }).superRefine((page, ctx) => {
  const invalid = () => ctx.addIssue({ code: 'custom', message: '카테고리 집계 계약 불일치' });
  if (Date.parse(page.to) - Date.parse(page.from) !== 300000 || Date.parse(page.to) % 60000 !== 0 || Date.parse(page.asOf) < Date.parse(page.to)) invalid();
  if (new Set(page.categories.map(c => c.categoryKey)).size !== page.categories.length) invalid();
  const denominator = page.categories.reduce((sum, c) => sum + (c.count ?? 0), 0);
  for (const c of page.categories) {
    const healthy = c.channels.filter(ch => ch.count !== null);
    const included = healthy.reduce((sum, ch) => sum + ch.count!, 0);
    if (c.observedChannels !== c.channels.length || c.healthyChannels !== healthy.length || new Set(c.channels.map(ch => ch.channelId)).size !== c.channels.length ||
        c.referenceCount !== c.channels.reduce((sum, ch) => sum + ch.referenceCount, 0) || c.count !== (healthy.length ? included : null) ||
        c.channels.some(ch => (ch.quality === 'HEALTHY') !== (ch.count !== null) || (ch.count !== null && ch.count !== ch.referenceCount)) ||
        (page.status === 'LAGGING' && healthy.length > 0)) invalid();
    const share = c.count === null || denominator === 0 ? null : c.count / denominator;
    if (share === null ? c.share !== null : c.share === null || Math.abs(c.share - share) > 1e-12) invalid();
  }
  if (page.unknownReferenceCount !== (page.categories.find(c => c.categoryKey === 'unknown')?.referenceCount ?? 0)) invalid();
});
export const categoryTimelineSchema = z.object({ categoryKey, name: z.string().min(1).max(120), asOf: timestamp, from: timestamp, to: timestamp,
  status, revision: count, scope: z.literal('COLLECTED_CHANNELS_ONLY'), truncated: z.boolean(),
  points: z.array(z.object({ from: timestamp, to: timestamp, count: count.nullable(), referenceCount: count, healthyChannels: count.max(20), observedChannels: count.max(20) })).length(60)
}).superRefine((page, ctx) => {
  if (Date.parse(page.to) - Date.parse(page.from) !== 3600000 || Date.parse(page.asOf) < Date.parse(page.to) ||
      !isMinuteTimeline(page.points, Date.parse(page.from), Date.parse(page.to)) ||
      page.points.some(p => p.healthyChannels > p.observedChannels || (p.count === null) !== (p.healthyChannels === 0) || (p.count !== null && p.count > p.referenceCount) || (page.status === 'LAGGING' && p.count !== null)))
    ctx.addIssue({ code: 'custom', message: '카테고리 시간축 계약 불일치' });
});
export type CategoryEntry = z.infer<typeof entry>;
export type CategoryTimeline = z.infer<typeof categoryTimelineSchema>;
export const fetchCategories = (signal: AbortSignal) => read('/api/v1/categories', categoriesSchema, signal);
export async function fetchCategoryTimeline(key: string, signal: AbortSignal) {
  if (!categoryKey.safeParse(key).success) throw new ApiError('contract');
  const result = await read(`/api/v1/categories/${key}/timeline`, categoryTimelineSchema, signal);
  if (result.categoryKey !== key) throw new ApiError('contract');
  return result;
}
