import { expect, it } from 'vitest';
import { parseRead } from './useReadNotifications';
import { canHighlight, notificationsSchema } from '../../shared/api/notifications';
const now = Date.parse('2026-10-25T12:00:00Z');
const event = { spikeId: '00000000-0000-4000-8000-000000000001', channelId: 'a'.repeat(32), evaluatedAt: new Date(now - 60000).toISOString(), receivedAt: new Date(now).toISOString(), count: 100, baseline: 50, ratio: 2, algorithmVersion: 'chat-spike-v1' as const, valid: true, qualityWarning: null, delayed: false };
it('알림 중복과 지연 여부의 모순을 거부한다', () => {
  expect(notificationsSchema.safeParse({ asOf: event.receivedAt, events: [event, event], nextCursor: null }).success).toBe(false);
  expect(notificationsSchema.safeParse({ asOf: event.receivedAt, events: [{ ...event, delayed: true }], nextCursor: null }).success).toBe(false);
});
it('10분 지연 경계의 마이크로초와 시간대 차이를 보존한다', () => {
  const parse = (receivedAt: string, delayed: boolean) => notificationsSchema.safeParse({
    asOf: receivedAt, events: [{ ...event, receivedAt, delayed }], nextCursor: null,
  }).success;
  expect(parse('2026-10-25T12:09:00Z', false)).toBe(true);
  expect(parse('2026-10-25T12:09:00.000001Z', true)).toBe(true);
  expect(parse('2026-10-25T21:09:00.000001+09:00', true)).toBe(true);
  expect(parse('2026-10-25T12:08:59.999999Z', false)).toBe(true);
  expect(parse('2026-10-25T12:09:00.000001Z', false)).toBe(false);
  expect(parse('invalid', false)).toBe(false);
});
it('최근 유효한 기록만 강조하고 미래·만료·정정·지연 알림은 보류한다', () => {
  expect(canHighlight(event, now)).toBe(true);
  expect(canHighlight(event, now + 600001)).toBe(false);
  expect(canHighlight(event, now - 120000)).toBe(false);
  expect(canHighlight({ ...event, valid: false }, now)).toBe(false);
  expect(canHighlight({ ...event, delayed: true }, now)).toBe(false);
});
it('읽음 상태는 손상·미래·7일 초과 기록을 제거한다', () => {
  expect(parseRead('{', now)).toEqual({});
  expect(parseRead(JSON.stringify({ [event.spikeId]: now }), now)).toEqual({ [event.spikeId]: now });
  for (const at of [now + 1, now - 8 * 86400000]) expect(parseRead(JSON.stringify({ [event.spikeId]: at }), now)).toEqual({});
});
