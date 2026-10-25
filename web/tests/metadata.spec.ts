import { expect, test, type Page } from '@playwright/test';
const a = 'a'.repeat(32), b = 'b'.repeat(32);
const now = Date.parse('2026-10-25T10:00:00Z');
function metadata(id = a) {
  return { channelId: id, channelName: id === a ? '숲의 친구' : '합성 요리사', title: id === a ? '숲속에서 함께 모험해요' : '따뜻한 저녁 만들기', categoryId: id === a ? 'game' : 'cook', categoryName: id === a ? '모험 게임' : '요리',
    thumbnailUrl: 'https://livecloud.pstatic.net/synthetic.jpg', liveStatus: 'LIVE', freshness: 'FRESH', reason: 'NONE', checkedAt: new Date(now).toISOString(), expiresAt: new Date(now + 60000).toISOString() };
}
async function setup(page: Page) {
  await page.clock.install({ time: now });
  await page.route('https://livecloud.pstatic.net/**', route => route.fulfill({ contentType: 'image/svg+xml', body: '<svg xmlns="http://www.w3.org/2000/svg" width="480" height="270"><rect width="480" height="270" fill="#bfdcc7"/><text x="100" y="140" font-size="24">Synthetic broadcast</text></svg>' }));
  await page.route('**/api/channels**', route => {
    const url = new URL(route.request().url());
    const id = url.pathname.split('/')[3];
    const requested = url.searchParams.get('ids')?.split(',') ?? [a, b];
    return route.fulfill({ json: id ? metadata(id) : { asOf: new Date(now).toISOString(), scope: 'COLLECTED_CHANNELS_ONLY', channels: requested.map(metadata) } });
  });
  await page.route('**/api/status', route => route.fulfill({ json: { asOf: new Date(now).toISOString(), scope: 'COLLECTED_CHANNELS_ONLY', coverageMeasured: false, consumer: { running: true, state: 'RUNNING', assignedPartitions: 3 } } }));
  await page.route('**/api/buckets?**', route => route.fulfill({ json: { asOf: new Date(now).toISOString(), windowSeconds: Number(new URL(route.request().url()).searchParams.get('seconds')), bucketSeconds: 10, includesOpenBucket: true, coverageMeasured: false, buckets: [] } }));
}
test('실제 계약의 제목·이미지·검색·카테고리·관심·상세를 연결한다', async ({ page }) => {
  await setup(page); await page.goto('/explore');
  await expect(page.locator('.broadcast-card')).toHaveCount(2);
  await expect(page.getByAltText('숲속에서 함께 모험해요 방송 미리보기')).toBeVisible();
  await expect(page.locator('.broadcast-card').first()).toContainText('최근 집계 없음');
  await expect(page.locator('.broadcast-card').first().locator('.card-bottom strong')).toHaveText('—');
  await page.getByRole('button', { name: '모험 게임', exact: true }).click();
  await expect(page.locator('.broadcast-card')).toHaveCount(1);
  await page.reload(); await expect(page.locator('.broadcast-card')).toHaveCount(1);
  await page.getByRole('button', { name: '숲의 친구 관심 방송 저장' }).click();
  await page.getByRole('navigation').getByRole('link', { name: /관심 방송/ }).click();
  await expect(page.getByRole('heading', { name: '다시 만나고 싶은 방송' })).toBeVisible();
  await expect(page.locator('.broadcast-card')).toHaveCount(1);
  await page.getByRole('searchbox').fill('저녁'); await page.getByRole('searchbox').press('Enter');
  await expect(page.locator('.broadcast-card')).toHaveCount(1);
  await expect(page).toHaveURL(/q=/);
  await expect(page.locator('.broadcast-card')).toHaveText([/따뜻한 저녁 만들기/]);
  await page.locator('.card-visual').click();
  await expect(page.getByRole('heading', { name: '합성 요리사', exact: true })).toBeVisible();
  await expect(page.locator('.detail-visual img')).toBeVisible();
  await page.reload(); await expect(page.locator('.detail-visual img')).toBeVisible();
});
test('이미지 로드 실패는 대체 화면으로 바뀌고 방송 상태는 유지한다', async ({ page }) => {
  await setup(page); await page.route('https://livecloud.pstatic.net/**', route => route.fulfill({ status: 404, body: '' }));
  await page.goto('/explore');
  await expect(page.locator('.broadcast-card img')).toHaveCount(0);
  await expect(page.getByText('방송 이미지 준비 중')).toHaveCount(2);
  await expect(page.getByText('방송 중', { exact: true })).toHaveCount(2);
});
test('성인·접근 제한으로 바뀌면 이전 제목과 이미지를 즉시 제거한다', async ({ page }) => {
  await setup(page); await page.goto('/explore');
  await expect(page.locator('.broadcast-card img')).toHaveCount(2);
  await page.route('**/api/channels**', route => route.fulfill({ json: { asOf: new Date(now).toISOString(), scope: 'COLLECTED_CHANNELS_ONLY', channels: [a, b].map(channelId => ({ ...metadata(channelId), channelName: null, title: null, categoryId: null, categoryName: null, thumbnailUrl: null, liveStatus: 'RESTRICTED', reason: 'RESTRICTED' })) } }));
  await page.clock.fastForward(5000);
  await expect(page.locator('.broadcast-card img')).toHaveCount(0);
  await expect(page.getByText('성인·접근 제한', { exact: true })).toHaveCount(2);
  await expect(page.getByText('숲속에서 함께 모험해요', { exact: true })).toHaveCount(0);
  await expect(page.getByRole('button', { name: '모험 게임', exact: true })).toHaveCount(0);
});
test('메타데이터 갱신 실패·만료가 채팅 조회를 막거나 LIVE를 무기한 유지하지 않는다', async ({ page }) => {
  await setup(page); await page.goto('/explore');
  await expect(page.locator('.broadcast-card img')).toHaveCount(2);
  await page.route('**/api/channels**', route => route.fulfill({ status: 503, body: '' }));
  await page.clock.fastForward(61000);
  await expect(page.locator('.broadcast-card img')).toHaveCount(0);
  await expect(page.getByText('이전 방송 정보 · 상태 재확인 중')).toHaveCount(2);
  await expect(page.getByText('방송 중', { exact: true })).toHaveCount(0);
  await expect(page.getByText('방송 제목과 이미지를 갱신하지 못했어요.', { exact: false })).toBeVisible();
  await page.clock.fastForward(240000);
  await expect(page.getByText('숲속에서 함께 모험해요', { exact: true })).toHaveCount(0);
});
for (const width of [320, 768, 1440]) {
  test(`${width}px 실제 제목·카테고리·상태가 있는 카드도 가로 넘침이 없다`, async ({ page }) => {
    await setup(page); await page.setViewportSize({ width, height: 900 }); await page.goto('/explore');
    await expect(page.locator('.broadcast-card img')).toHaveCount(2);
    expect(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth)).toBe(false);
  });
}
