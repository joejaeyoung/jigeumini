import { test, expect } from '@playwright/test';
const end = Date.parse('2026-10-25T12:00:00Z'), iso = (n: number) => new Date(n).toISOString(), key = 'a'.repeat(64), channel = 'a'.repeat(32);
test('카테고리 비중·품질·시간대 공백·기여 방송과 모바일 이동을 표시한다', async ({ page }) => {
  await page.clock.install({ time: end + 8000 }); await page.setViewportSize({ width: 320, height: 900 });
  await page.route('**/api/channels?*', route => route.fulfill({ json: { asOf: iso(end), scope: 'REQUESTED_CHANNELS', channels: [{ channelId: channel, channelName: '합성 기여 방송', title: null, categoryId: null, categoryName: null, thumbnailUrl: null, liveStatus: 'UNKNOWN', freshness: 'UNAVAILABLE', reason: 'PENDING', checkedAt: null, expiresAt: null }] } }));
  await page.route('**/api/v1/categories', route => route.fulfill({ json: { asOf: iso(end + 8000), from: iso(end - 300000), to: iso(end), scope: 'COLLECTED_CHANNELS_ONLY', status: 'READY', revision: 2, unknownReferenceCount: 0, truncated: false,
    categories: [{ categoryKey: key, name: '합성 게임', count: 6, referenceCount: 6, observedChannels: 1, healthyChannels: 1, share: 1,
      channels: [{ channelId: channel, count: 6, referenceCount: 6, quality: 'HEALTHY' }] }] } }));
  await page.route('**/api/v1/categories/*/timeline', route => route.fulfill({ json: { categoryKey: key, name: '합성 게임', asOf: iso(end + 8000), from: iso(end - 3600000), to: iso(end), scope: 'COLLECTED_CHANNELS_ONLY', status: 'READY', revision: 2, truncated: false,
    points: Array.from({ length: 60 }, (_, i) => ({ from: iso(end - 3600000 + i * 60000), to: iso(end - 3600000 + (i + 1) * 60000), count: i === 1 ? null : i, referenceCount: i, healthyChannels: i === 1 ? 0 : 1, observedChannels: 1 })) } }));
  await page.goto('/categories'); await expect(page.locator('.category-row')).toContainText('100.0%');
  await expect(page.locator('.category-row')).toContainText('품질 충족 1 / 관측 1채널');
  await page.getByRole('link', { name: '합성 게임', exact: true }).click();
  await expect(page.locator('.category-gap')).toHaveCount(1); await expect(page.locator('.category-bar')).toHaveCount(59);
  await expect(page.getByRole('link', { name: '합성 기여 방송' })).toHaveAttribute('href', `/channels/${channel}`);
  await page.getByText('분별 건수와 품질 보기', { exact: true }).click();
  await expect(page.getByRole('row').nth(1)).toContainText('0'); await expect(page.getByRole('row').nth(2)).toContainText('—');
  expect(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth)).toBe(false);
  await page.locator('.categories-page').screenshot({ path: 'test-results/categories-320.png' });
  await page.setViewportSize({ width: 1440, height: 1000 }); await page.locator('.categories-page').screenshot({ path: 'test-results/categories-1440.png' });
  await page.route('**/api/v1/categories/*/timeline', route => route.fulfill({ status: 503 }));
  await page.clock.fastForward(5000); await expect(page.getByRole('alert')).toContainText('이전 조회 결과');
});
test('카테고리 빈 상태와 소비 지연을 0건으로 표시하지 않는다', async ({ page }) => {
  const base = { asOf: iso(end + 8000), from: iso(end - 300000), to: iso(end), scope: 'COLLECTED_CHANNELS_ONLY', status: 'LAGGING', revision: 0, unknownReferenceCount: 0, truncated: false, categories: [] };
  await page.route('**/api/v1/categories', route => route.fulfill({ json: base })); await page.goto('/categories');
  await expect(page.getByRole('heading', { name: '아직 카테고리 관측이 없어요' })).toBeVisible();
  await expect(page.getByText('카테고리 분석의 진행을 확인하지 못했어요. 건수와 비중은 확인 후 표시해요.')).toBeVisible();
  await expect(page.locator('.category-row')).toHaveCount(0);
});
