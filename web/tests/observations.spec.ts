import { expect, test } from '@playwright/test';
const id = 'a'.repeat(32), end = Date.parse('2026-10-25T12:00:00Z');
const iso = (millis: number) => new Date(millis).toISOString();
test('한 시간 관측점과 채팅 품질 공백·기간 변경·빈 상태·모바일을 표시한다', async ({ page }) => {
  await page.clock.install({ time: end }); await page.setViewportSize({ width: 320, height: 900 });
  await page.route('**/api/channels/**', route => route.fulfill({ json: { channelId: id, channelName: '합성 관측 방송', title: '테스트 방송', categoryId: null, categoryName: null, thumbnailUrl: null,
    liveStatus: 'LIVE', freshness: 'FRESH', reason: 'NONE', checkedAt: iso(end), expiresAt: iso(end + 60000) } }));
  await page.route('**/api/v1/channels/*/timeline?**', route => {
    const seconds = Number(new URL(route.request().url()).searchParams.get('seconds')), start = end - seconds * 1000;
    const observations = seconds === 1800 ? [] : [0, 60, 120, 180, 360, 420].map((offset, i) => ({
      schemaVersion: 1, eventId: `00000000-0000-4000-8000-${String(i + 1).padStart(12, '0')}`, runId: 'test-v1', channelId: id,
      observedAt: iso(start + (offset + 7) * 1000), ttlSeconds: 120, viewerCount: i === 2 ? null : i * 100, quality: i === 2 ? 'MISSING_VIEWER_COUNT' : 'OBSERVED',
      liveStatus: 'LIVE', sourceBroadcastId: '123', broadcastIdentity: 'SOURCE_PROVIDED', categoryId: null, categoryName: null,
    }));
    return route.fulfill({ json: { channelId: id, asOf: iso(end), from: iso(start), to: iso(end), truncated: false, observations,
      chat: Array.from({ length: seconds / 60 }, (_, i) => ({ from: iso(start + i * 60000), to: iso(start + (i + 1) * 60000), count: i === 2 ? null : i * 2,
        referenceCount: i * 2, coverage: i === 2 ? .5 : 1, status: i === 2 ? 'INCOMPLETE' : 'HEALTHY', reasons: i === 2 ? ['LOW_COVERAGE'] : [] })) } });
  });
  await page.goto(`/channels/${id}`);
  await expect(page.locator('.viewer-point')).toHaveCount(5); await expect(page.locator('.viewer-line')).toHaveCount(2);
  await expect(page.locator('.chat-gap')).toHaveCount(1); await expect(page.getByRole('table')).toContainText('최근 60분');
  const picker = page.getByLabel('시청자 관측값 선택');
  await picker.focus(); await expect(picker).toBeFocused();
  expect((await picker.boundingBox())!.height).toBeGreaterThanOrEqual(44);
  await picker.selectOption('00000000-0000-4000-8000-000000000001');
  await expect(page.locator('.observation-selection')).toContainText('시청자 0명');
  expect(await page.locator('.viewer-point').first().getAttribute('cx')).toBe(String(7 / 3600 * 1000));
  expect(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth)).toBe(false);
  await page.locator('.observation-timeline').screenshot({ path: 'test-results/observations-320.png' });
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.locator('.observation-timeline').screenshot({ path: 'test-results/observations-1440.png' });
  await page.getByRole('button', { name: '30분', exact: true }).click();
  await expect(page.getByText('이 시간대에는 시청자 관측값이 없어요.')).toBeVisible(); await expect(page.locator('.viewer-point')).toHaveCount(0);
  await page.route('**/api/v1/channels/*/timeline?**', route => route.fulfill({ status: 503 }));
  await page.clock.fastForward(5000); await expect(page.getByRole('alert')).toContainText('이전 조회 결과');
});
