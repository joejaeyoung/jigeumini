import { test, expect } from '@playwright/test';
import { readFileSync } from 'node:fs';
const fixture = JSON.parse(readFileSync(new URL('../../server/src/test/resources/broadcast-report.json', import.meta.url), 'utf8'));
const channel = fixture.header.channelId, id = fixture.header.reportId;
test('방송 기록에서 부분 관측 리포트로 이동하고 0건·공백·피크와 이전 결과를 표시한다', async ({ page }) => {
  await page.clock.install({ time: new Date(fixture.asOf) }); await page.setViewportSize({ width: 320, height: 900 });
  await page.route(`**/api/v1/channels/${channel}/reports`, route => route.fulfill({ json: { channelId: channel, asOf: fixture.asOf, status: 'READY', retentionDays: 7, truncated: false, unassignedCount: 0, reports: [fixture.header] } }));
  await page.route(`**/api/v1/reports/${id}`, route => route.fulfill({ json: fixture }));
  await page.goto(`/channels/${channel}/reports`); await expect(page.getByRole('heading', { name: '방송별 요약', exact: true })).toBeVisible();
  await page.locator('.report-list a').click(); await expect(page.getByRole('heading', { name: '방송 요약', exact: true })).toBeVisible();
  await expect(page.locator('.report-range')).toContainText('일부 구간의 관측 결과'); await expect(page.locator('.report-range')).toContainText('종료 상태 관측');
  await expect(page.locator('.report-metrics')).toContainText('14'); await expect(page.locator('.category-gap')).toHaveCount(1);
  await expect(page.locator('.report-peaks li')).toHaveCount(1); await expect(page.locator('.report-peaks')).toContainText('분당 최대 3건');
  await page.getByText('분별 건수와 품질 보기', { exact: true }).click();
  await expect(page.getByRole('row').nth(1)).toContainText('0'); await expect(page.getByRole('row').nth(2)).toContainText('—');
  expect(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth)).toBe(false);
  await page.locator('.reports-page').screenshot({ path: 'test-results/reports-320.png' });
  await page.setViewportSize({ width: 1440, height: 1000 }); await page.locator('.reports-page').screenshot({ path: 'test-results/reports-1440.png' });
  await page.route(`**/api/v1/reports/${id}`, route => route.fulfill({ status: 503 })); await page.clock.fastForward(15000);
  await expect(page.getByRole('alert')).toContainText('이전 조회 결과');
});
test('내부 관측 세션의 미확정 종료와 분석 지연을 확정 수치로 표시하지 않는다', async ({ page }) => {
  const report = { ...fixture, header: { ...fixture.header, identity: 'INTERNAL_OBSERVATION_SESSION', state: 'UNKNOWN', endedAt: null, endReason: 'UNCONFIRMED', identityUnconfirmed: true, viewerMaximum: null },
    status: 'LAGGING', qualifiedCount: null, maximumPerMinute: null, healthyMinutes: 0, peaks: [],
    points: fixture.points.map((p: { reasons: string[] }) => ({ ...p, count: null, reasons: [...p.reasons, 'REPORT_LAGGING'] })) };
  await page.route(`**/api/v1/reports/${id}`, route => route.fulfill({ json: report })); await page.goto(`/reports/${id}`);
  await expect(page.locator('.report-range')).toContainText('종료 미확정'); await expect(page.locator('.report-range')).toContainText('내부 관측 세션');
  await expect(page.locator('.report-metrics dd').nth(1)).toHaveText('— 건'); await expect(page.locator('.report-metrics dd').nth(3)).toHaveText('— 명');
  await expect(page.locator('.category-gap')).toHaveCount(3); await expect(page.locator('.report-peaks')).toHaveCount(0);
});
test('방송 기록 빈 상태를 과거 데이터나 0건으로 채우지 않는다', async ({ page }) => {
  await page.route(`**/api/v1/channels/${channel}/reports`, route => route.fulfill({ json: { channelId: channel, asOf: fixture.asOf, status: 'READY', retentionDays: 7, truncated: false, unassignedCount: 0, reports: [] } }));
  await page.goto(`/channels/${channel}/reports`); await expect(page.getByRole('heading', { name: '아직 방송 관측 기록이 없어요' })).toBeVisible();
  await expect(page.locator('.report-list')).toHaveCount(0);
});
