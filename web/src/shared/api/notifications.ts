import { z } from 'zod';
import { channelIdSchema, timestamp } from './fields';
import { spikeSchema } from './quality';
import { ApiError, read } from './transport';
function instantNanos(value: string) {
  // Date는 밀리초 아래를 버리므로 DB의 마이크로초를 별도로 더한다.
  const fraction = value.match(/\.(\d+)/)?.[1] ?? '';
  return BigInt(Date.parse(value)) * 1000000n + BigInt(fraction.padEnd(9, '0').slice(3, 9));
}
export const notificationSchema = z.object({ ...spikeSchema.shape, receivedAt: timestamp, delayed: z.boolean() }).superRefine((event, ctx) => {
  if (!timestamp.safeParse(event.receivedAt).success) return;
  if (!spikeSchema.safeParse(event).success || event.delayed !== (instantNanos(event.receivedAt) > instantNanos(event.evaluatedAt) + 600000000000n))
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
