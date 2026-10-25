import { z } from 'zod';
import { channelIdSchema, timestamp } from './fields';
import { spikeSchema } from './quality';
import { ApiError, read } from './transport';
export const notificationSchema = z.object({ ...spikeSchema.shape, receivedAt: timestamp, delayed: z.boolean() }).superRefine((event, ctx) => {
  if (!spikeSchema.safeParse(event).success || event.delayed !== (Date.parse(event.receivedAt) > Date.parse(event.evaluatedAt) + 600000))
    ctx.addIssue({ code: 'custom', message: '알림 판정 계약 불일치' });
});
export const notificationsSchema = z.object({ asOf: timestamp, events: z.array(notificationSchema).max(100), nextCursor: z.string().nullable() }).superRefine((page, ctx) => {
  if (new Set(page.events.map(event => event.spikeId)).size !== page.events.length) ctx.addIssue({ code: 'custom', message: '중복 알림' });
});
export type Notification = z.infer<typeof notificationSchema>;
export async function fetchNotifications(channels: string[], signal: AbortSignal, cursor = '') {
  if (channels.length > 20 || channels.some(id => !channelIdSchema.safeParse(id).success)) throw new ApiError('contract');
  const page = await read(`/api/v1/notifications?${new URLSearchParams({ channels: channels.join(','), cursor, limit: '20' })}`, notificationsSchema, signal);
  if (page.events.some(event => !channels.includes(event.channelId))) throw new ApiError('contract');
  return page;
}
export function canHighlight(event: Notification, now: number) {
  const evaluated = Date.parse(event.evaluatedAt);
  return event.valid && !event.delayed && now >= evaluated && now <= evaluated + 600000;
}
