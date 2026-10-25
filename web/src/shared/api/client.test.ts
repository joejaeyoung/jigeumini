import { afterEach, expect, it, vi } from 'vitest';
import { bucketsSchema, fetchBuckets, fetchSnapshot } from './client';
const response = { asOf: '2026-10-25T00:01:00Z', windowSeconds: 300, bucketSeconds: 10, includesOpenBucket: true, coverageMeasured: false, buckets: [] };
afterEach(() => vi.unstubAllGlobals());
it('잘못된 응답을 정상 빈 결과로 취급하지 않는다', () => {
  expect(bucketsSchema.safeParse({ ...response, buckets: [{ channelId: 'invalid', bucketStart: response.asOf, chatCount: -1 }] }).success).toBe(false);
  expect(bucketsSchema.safeParse({ ...response, asOf: 'yesterday' }).success).toBe(false);
});
it('조회 기간과 채널을 인코딩한 API 응답을 읽는다', async () => {
  const fetcher = vi.fn().mockResolvedValue(new Response(JSON.stringify(response)));
  vi.stubGlobal('fetch', fetcher);
  expect(await fetchBuckets(300, new AbortController().signal, 'a'.repeat(32))).toEqual(response);
  expect(fetcher.mock.calls[0][0]).toBe(`/api/buckets?seconds=300&channelId=${'a'.repeat(32)}`);
});
it('일부 API 실패를 성공 스냅샷으로 섞지 않는다', async () => {
  const fetcher = vi.fn().mockResolvedValue(new Response('', { status: 503 }));
  vi.stubGlobal('fetch', fetcher);
  await expect(fetchSnapshot(300, new AbortController().signal)).rejects.toMatchObject({ reason: 'response' });
  expect(fetcher.mock.calls.every(([, options]) => options.signal.aborted)).toBe(true);
});
it('응답의 기간·채널이 요청과 다르면 거부한다', async () => {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ ...response, windowSeconds: 60 }))));
  await expect(fetchBuckets(300, new AbortController().signal)).rejects.toMatchObject({ reason: 'contract' });
});
