import { readFileSync } from 'node:fs';
import { afterEach, expect, it, vi } from 'vitest';
import { fetchReport, reportSchema, reportListSchema } from './reports';
const fixture = JSON.parse(readFileSync(new URL('../../../../server/src/test/resources/broadcast-report.json', import.meta.url), 'utf8'));
afterEach(() => vi.unstubAllGlobals());
it('실제 Kafka/DB 리포트 조회가 만든 JSON의 정상 0건·공백·피크를 읽는다', async () => {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify(fixture))));
  const report = await fetchReport(fixture.header.reportId, new AbortController().signal);
  expect(report.points.map(p => p.count)).toEqual([0, null, 3]); expect(report.referenceCount).toBe(14); expect(report.qualifiedCount).toBe(3);
  expect(report.header.viewerMaximum).toBe(0); expect(report.peaks).toHaveLength(1);
});
it('미확인 건수·잘못된 완료 분·품질·피크를 성공 결과로 해석하지 않는다', () => {
  for (const invalid of [{ ...fixture, qualifiedCount: 14 }, { ...fixture, status: 'LAGGING' }, { ...fixture, metricsTo: fixture.metricsFrom },
    { ...fixture, points: fixture.points.slice(1) }, { ...fixture, peaks: [{ ...fixture.peaks[0], maximum: 14 }] },
    { ...fixture, points: fixture.points.map((p: object, i: number) => i ? p : { ...p, identityCoverage: .99 }) }])
    expect(reportSchema.safeParse(invalid).success).toBe(false);
  expect(reportListSchema.safeParse({ channelId: 'b'.repeat(32), asOf: fixture.asOf, status: 'READY', retentionDays: 7, truncated: false, unassignedCount: 0, reports: [fixture.header] }).success).toBe(false);
});
