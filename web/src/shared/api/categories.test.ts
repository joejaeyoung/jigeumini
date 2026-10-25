import { describe, expect, it } from 'vitest';
import { categoriesSchema, categoryTimelineSchema } from './categories';
const end = Date.parse('2026-10-25T12:00:00Z'), iso = (n: number) => new Date(n).toISOString();
const base = { asOf: iso(end + 8000), from: iso(end - 300000), to: iso(end), scope: 'COLLECTED_CHANNELS_ONLY', status: 'READY', revision: 1, unknownReferenceCount: 0, truncated: false };
const zero = { categoryKey: 'a'.repeat(64), name: '합성 게임', count: 0, referenceCount: 0, observedChannels: 1, healthyChannels: 1, share: null,
  channels: [{ channelId: 'a'.repeat(32), count: 0, referenceCount: 0, quality: 'HEALTHY' }] };
describe('카테고리 집계 계약', () => {
  it('정상 0건·미확인·빈 구간을 구분하고 합계와 비중을 검증한다', () => {
    expect(categoriesSchema.safeParse({ ...base, categories: [zero] }).success).toBe(true);
    expect(categoriesSchema.safeParse({ ...base, categories: [] }).success).toBe(true);
    const entry = { ...zero, count: null, healthyChannels: 0, channels: [{ ...zero.channels[0], count: null, quality: 'INCOMPLETE' }] };
    expect(categoriesSchema.safeParse({ ...base, status: 'LAGGING', categories: [entry] }).success).toBe(true);
    for (const changed of [{ count: 1 }, { referenceCount: 1 }, { healthyChannels: 0 }, { share: 1 }, { channels: [] }])
      expect(categoriesSchema.safeParse({ ...base, categories: [{ ...zero, ...changed }] }).success).toBe(false);
    expect(categoriesSchema.safeParse({ ...base, status: 'LAGGING', categories: [zero] }).success).toBe(false);
    expect(categoriesSchema.safeParse({ ...base, categories: [zero, zero] }).success).toBe(false);
  });
  it('한 시간의 연속된 분과 품질 채널 수를 대조한다', () => {
    const points = Array.from({ length: 60 }, (_, i) => ({ from: iso(end - 3600000 + i * 60000), to: iso(end - 3600000 + (i + 1) * 60000),
      count: i === 1 ? null : 0, referenceCount: 0, healthyChannels: i === 1 ? 0 : 1, observedChannels: 1 }));
    const page = { ...base, from: iso(end - 3600000), categoryKey: zero.categoryKey, name: zero.name, points };
    expect(categoryTimelineSchema.safeParse(page).success).toBe(true);
    for (const changed of [{ from: iso(end) }, { count: 0, healthyChannels: 0 }, { observedChannels: 0 }])
      expect(categoryTimelineSchema.safeParse({ ...page, points: [{ ...points[0], ...changed }, ...points.slice(1)] }).success).toBe(false);
    expect(categoryTimelineSchema.safeParse({ ...page, points: points.slice(1) }).success).toBe(false);
  });
});
