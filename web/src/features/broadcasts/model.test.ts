import { describe, expect, it } from 'vitest';
import { summarize, withSavedChannels } from './model';
const a = 'a'.repeat(32), b = 'b'.repeat(32), c = 'c'.repeat(32);
describe('수집 집계', () => {
  it('역순 버킷도 채널별 합계와 최신 구간을 보존한다', () => {
    const rows = summarize([
      { channelId: a, bucketStart: '2026-10-25T00:00:10Z', chatCount: 5 },
      { channelId: b, bucketStart: '2026-10-25T00:00:00Z', chatCount: 9 },
      { channelId: a, bucketStart: '2026-10-25T00:00:00Z', chatCount: 7 },
    ]);
    expect(rows).toEqual([{ id: a, chatCount: 12, lastBucket: '2026-10-25T00:00:10Z' }, { id: b, chatCount: 9, lastBucket: '2026-10-25T00:00:00Z' }]);
  });
  it('수집되지 않은 관심 채널은 0건을 만들지 않는다', () => {
    expect(withSavedChannels([], [c])).toEqual([{ id: c, chatCount: null, lastBucket: null }]);
    expect(summarize([])).toEqual([]);
  });
  it('정수 안전 범위를 넘는 합계를 표시하지 않는다', () => {
    expect(() => summarize([Number.MAX_SAFE_INTEGER, 1].map(chatCount => ({ channelId: a, bucketStart: '2026-10-25T00:00:00Z', chatCount })))).toThrow();
  });
});
