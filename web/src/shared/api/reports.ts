import { z } from 'zod';
import { channelIdSchema, count, timestamp } from './fields';
import { ApiError, read } from './transport';
import { isMinuteTimeline } from './minuteTimeline';

export const reportId = z.uuid();
const header = z.object({ reportId, channelId: channelIdSchema, identity: z.enum(['SOURCE_BROADCAST', 'INTERNAL_OBSERVATION_SESSION']),
  state: z.enum(['LIVE', 'OFFLINE', 'UNKNOWN']), endReason: z.enum(['UNCONFIRMED', 'OFFLINE_OBSERVED', 'SOURCE_CHANGED']), identityUnconfirmed: z.boolean(),
  observedFrom: timestamp, observedTo: timestamp, endedAt: timestamp.nullable(), viewerMaximum: count.nullable(),
  reportVersion: z.string().regex(/^(data-\d+|report-v1-[a-f0-9]{64})$/), updatedAt: timestamp }).superRefine((h, ctx) => {
  if (Date.parse(h.observedFrom) > Date.parse(h.observedTo) || (h.endedAt !== null && Date.parse(h.endedAt) < Date.parse(h.observedTo)) ||
      (h.state === 'OFFLINE') !== (h.endedAt !== null) || (h.endReason === 'UNCONFIRMED') !== (h.endedAt === null))
    ctx.addIssue({ code: 'custom', message: '관측 세션 계약 불일치' });
});
const status = z.enum(['READY', 'LAGGING']);
export const reportListSchema = z.object({ channelId: channelIdSchema, asOf: timestamp, status, retentionDays: z.literal(7), truncated: z.boolean(),
  unassignedCount: count, reports: z.array(header).max(100) }).superRefine((page, ctx) => {
  if (page.reports.some(h => h.channelId !== page.channelId) || new Set(page.reports.map(h => h.reportId)).size !== page.reports.length)
    ctx.addIssue({ code: 'custom', message: '방송 기록 목록 계약 불일치' });
});
export const reportSchema = z.object({ header, asOf: timestamp, status, scope: z.literal('OBSERVED_ONLY'), metricsFrom: timestamp, metricsTo: timestamp,
  partialObservation: z.literal(true), referenceCount: count, qualifiedCount: count.nullable(), maximumPerMinute: count.nullable(),
  completedMinutes: count, healthyMinutes: count, lateCount: count, unassignedChannelCount: count,
  peaks: z.array(z.object({ from: timestamp, to: timestamp, peakAt: timestamp, maximum: count.positive() })).max(3),
  points: z.array(z.object({ from: timestamp, to: timestamp, count: count.nullable(), referenceCount: count, collectionCoverage: z.number().min(0).max(1), identityCoverage: z.number().min(0).max(1),
    reasons: z.array(z.enum(['KNOWN_LOSS', 'LOW_COVERAGE', 'QUALITY_UNCONFIRMED', 'BROADCAST_IDENTITY_GAP', 'REPORT_LAGGING'])).max(3) })).max(360), timelineTruncated: z.boolean()
}).superRefine((report, ctx) => {
  const invalid = () => ctx.addIssue({ code: 'custom', message: '방송 요약 계약 불일치' });
  const from = Date.parse(report.metricsFrom), to = Date.parse(report.metricsTo), visibleFrom = Math.max(from, to - 360 * 60000);
  if (to < from || from % 60000 !== 0 || to % 60000 !== 0 || to > Date.parse(report.asOf) || (to - from) / 60000 !== report.completedMinutes ||
      report.healthyMinutes > report.completedMinutes || (report.qualifiedCount === null) !== (report.healthyMinutes === 0) ||
      (report.maximumPerMinute === null) !== (report.healthyMinutes === 0) || (report.qualifiedCount !== null && report.qualifiedCount > report.referenceCount) ||
      (report.maximumPerMinute !== null && report.maximumPerMinute > report.qualifiedCount!) || report.lateCount > report.referenceCount ||
      report.timelineTruncated !== (report.completedMinutes > 360) || !isMinuteTimeline(report.points, visibleFrom, to) ||
      !report.header.reportVersion.startsWith('report-v1-') || (report.status === 'LAGGING' && report.healthyMinutes > 0)) invalid();
  report.points.forEach(p => {
    const healthy = p.reasons.length === 0;
    if (healthy !== (p.count !== null) || (healthy && (p.count !== p.referenceCount || p.collectionCoverage < .95 || p.identityCoverage !== 1)) ||
        (report.status === 'LAGGING' && !p.reasons.includes('REPORT_LAGGING'))) invalid();
  });
  if (!report.timelineTruncated) {
    const healthy = report.points.filter(p => p.count !== null), sum = healthy.reduce((n, p) => n + p.count!, 0);
    if (healthy.length !== report.healthyMinutes || report.qualifiedCount !== (healthy.length ? sum : null) ||
        report.maximumPerMinute !== (healthy.length ? Math.max(...healthy.map(p => p.count!)) : null)) invalid();
  }
  report.peaks.forEach((p, i) => {
    if (Date.parse(p.from) < from || Date.parse(p.to) > to || Date.parse(p.from) > Date.parse(p.peakAt) || Date.parse(p.peakAt) >= Date.parse(p.to) ||
        p.maximum > (report.maximumPerMinute ?? 0) || (i > 0 && (p.maximum > report.peaks[i - 1].maximum || (p.maximum === report.peaks[i - 1].maximum && p.peakAt < report.peaks[i - 1].peakAt)))) invalid();
  });
});
export type ReportHeader = z.infer<typeof header>;
export type BroadcastReport = z.infer<typeof reportSchema>;
export async function fetchReports(channel: string, signal: AbortSignal) {
  if (!channelIdSchema.safeParse(channel).success) throw new ApiError('contract');
  const result = await read(`/api/v1/channels/${channel}/reports`, reportListSchema, signal);
  if (result.channelId !== channel) throw new ApiError('contract');
  return result;
}
export async function fetchReport(id: string, signal: AbortSignal) {
  if (!reportId.safeParse(id).success) throw new ApiError('contract');
  return read(`/api/v1/reports/${id}`, reportSchema, signal);
}
