import { expect, test, type Page } from '@playwright/test';
const a = 'a'.repeat(32), b = 'b'.repeat(32);
async function mockApi(page: Page, mode: 'filled' | 'empty' = 'filled') {
  await page.route('**/api/channels**', route => route.fulfill({ json: { asOf: new Date().toISOString(), scope: 'COLLECTED_CHANNELS_ONLY', channels: [] } }));
  await page.route('**/api/status', route => route.fulfill({ json: { asOf: '2026-10-25T00:05:00Z', scope: 'COLLECTED_CHANNELS_ONLY', coverageMeasured: false, consumer: { running: true, state: 'RUNNING', assignedPartitions: 3 } } }));
  await page.route('**/api/v1/rankings?**', route => {
    const seconds = Number(new URL(route.request().url()).searchParams.get('window'));
    const at = new Date(Math.floor(Date.now() / 10000) * 10000).toISOString();
    return route.fulfill({ json: { asOf: new Date().toISOString(), evaluatedAt: at, snapshotId: '00000000-0000-4000-8000-000000000001', windowSeconds: seconds,
      algorithmVersion: 'quality-ranking-v1', status: mode === 'empty' ? 'WARMING_UP' : 'READY', excludedChannels: [],
      entries: mode === 'empty' ? [] : [b, a].map((channelId, i) => ({ channelId, rank: i + 1, count: seconds === 60 ? 3 - i : 9 - i, referenceCount: seconds === 60 ? 3 - i : 9 - i, coverage: 1, status: 'HEALTHY', reasons: [], previousRank: null, previousReason: 'NO_PREVIOUS_SNAPSHOT' })) } });
  });
  await page.route('**/api/v1/channels/*/activity?**', route => {
    const url = new URL(route.request().url()), from = url.searchParams.get('from'), to = url.searchParams.get('to');
    return route.fulfill({ json: { channelId: url.pathname.split('/')[4], asOf: new Date().toISOString(), from, to,
      points: [{ from, to: new Date(Date.parse(from!) + 60000).toISOString(), count: 2, referenceCount: 2, coverage: 1, status: 'HEALTHY', reasons: [] }] } });
  });
  await page.route('**/api/v1/spikes?**', route => route.fulfill({ json: { asOf: new Date().toISOString(), status: 'WARMING_UP', events: [], nextCursor: null } }));
}
test('조회·기간 변경·검색·관심 저장·상세 및 직접 주소 복원', async ({ page }) => {
  await mockApi(page);
  await page.goto('/explore');
  await expect(page.locator('.broadcast-card')).toHaveCount(2);
  await expect(page.locator('.broadcast-card').first()).toContainText('bbbbbbbb');
  await page.getByRole('button', { name: '최근 1분', exact: true }).click();
  await expect(page.locator('.broadcast-card').first().locator('.card-bottom strong')).toHaveText('3');
  await page.getByRole('button', { name: '수집 채널 aaaaaaaa 관심 방송 저장', exact: true }).click();
  await page.getByRole('navigation').getByRole('link', { name: /관심 방송/ }).click();
  await expect(page.locator('.broadcast-card')).toHaveCount(1);
  await page.reload();
  await expect(page.locator('.broadcast-card')).toHaveCount(1);
  await page.getByRole('searchbox').fill('bbbb');
  await page.getByRole('searchbox').press('Enter');
  await expect(page.locator('.broadcast-card')).toHaveCount(1);
  await expect(page.locator('.broadcast-card')).toContainText('bbbbbbbb');
  await page.locator('.card-visual').click();
  await expect(page.getByRole('heading', { name: '수집 채널 bbbbbbbb', exact: true })).toBeVisible();
  await expect(page.getByRole('table')).toBeVisible();
  await page.reload();
  await expect(page.getByRole('table')).toBeVisible();
  await page.getByRole('button', { name: '60분', exact: true }).click();
  await expect(page.getByRole('table')).toContainText('최근 60분');
});
test('빈 결과와 서버 오류를 구분하고 재시도한다', async ({ page }) => {
  await mockApi(page, 'empty');
  await page.goto('/explore');
  await expect(page.getByText('아직 이 시간대에 수집된 방송이 없어요')).toBeVisible();
  await page.route('**/api/v1/rankings?**', route => route.fulfill({ status: 503, body: '' }));
  await page.reload();
  await expect(page.getByRole('alert')).toContainText('불러오지 못했어요');
  await expect(page.getByText('아직 이 시간대에 수집된 방송이 없어요')).toHaveCount(0);
  await page.unroute('**/api/v1/rankings?**');
  await mockApi(page);
  await page.getByRole('button', { name: '다시 시도' }).click();
  await expect(page.locator('.broadcast-card')).toHaveCount(2);
});
test('갱신 실패는 마지막 데이터와 조회 시각을 유지하고 오래된 값임을 알린다', async ({ page }) => {
  await page.clock.install();
  await mockApi(page);
  await page.goto('/explore');
  await expect(page.locator('.broadcast-card')).toHaveCount(2);
  await page.route('**/api/v1/rankings?**', route => route.fulfill({ status: 500, body: '' }));
  await page.clock.fastForward(5000);
  await expect(page.getByRole('alert')).toContainText('이전 조회 결과', { timeout: 12000 });
  await expect(page.locator('.broadcast-card')).toHaveCount(2);
});
test('최근 기록 없는 관심 채널은 미확인 상태이며 급상승 배수를 꾸미지 않는다', async ({ page }) => {
  await mockApi(page, 'empty');
  await page.addInitScript(id => localStorage.setItem('jigeumini.saved-channels.v1', JSON.stringify([id])), a);
  await page.goto('/saved');
  await expect(page.locator('.broadcast-card')).toContainText('최근 집계 없음');
  await expect(page.locator('.card-bottom strong')).toHaveText('—');
  await page.getByRole('navigation').getByRole('link', { name: '반응 급상승' }).click();
  await expect(page.getByText('반응을 비교할 기록을 모으고 있어요')).toBeVisible();
  await expect(page.locator('.reaction-ribbon')).toHaveCount(0);
});
for (const width of [320, 375, 414, 768, 1440]) {
  test(`${width}px 상단 메뉴·카드·모달 가로 넘침과 키보드 닫기`, async ({ page }) => {
    await page.setViewportSize({ width, height: 900 });
    await mockApi(page);
    await page.goto('/');
    await expect(page.locator('.broadcast-card')).toHaveCount(2);
    const overflow = await page.evaluate(() => document.documentElement.scrollWidth > innerWidth || [...document.querySelectorAll('nav a,.broadcast-card')].some(el => el.getBoundingClientRect().right > innerWidth));
    expect(overflow).toBe(false);
    await page.getByRole('button', { name: '서비스와 집계 기준' }).click();
    await expect(page.getByRole('dialog')).toBeVisible();
    await page.keyboard.press('Escape');
    await expect(page.getByRole('dialog')).toHaveCount(0);
    await expect(page.getByRole('button', { name: '서비스와 집계 기준' })).toBeFocused();
  });
}

test('관심 방송 조회 중·실패를 최근 집계 없음으로 표시하지 않는다', async ({ page }) => {
  await mockApi(page);
  await page.addInitScript(id => localStorage.setItem('jigeumini.saved-channels.v1', JSON.stringify([id])), a);
  let release!: () => void;
  const pending = new Promise<void>(resolve => { release = resolve; });
  await page.route('**/api/v1/rankings?**', async route => { await pending; await route.fulfill({ status: 503, body: '' }); });
  await page.goto('/saved');
  await expect(page.getByText('수집된 방송을 불러오는 중…')).toBeVisible();
  await expect(page.locator('.broadcast-card')).toHaveCount(0);
  await expect(page.getByText('최근 집계 없음')).toHaveCount(0);
  release();
  await expect(page.getByRole('alert')).toContainText('불러오지 못했어요');
  await expect(page.getByText('최근 집계 없음')).toHaveCount(0);
  await page.unroute('**/api/v1/rankings?**');
  await mockApi(page, 'empty');
  await page.getByRole('button', { name: '다시 시도' }).click();
  await expect(page.locator('.broadcast-card')).toContainText('최근 집계 없음');
});
