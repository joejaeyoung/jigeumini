import { expect, test, type Page } from '@playwright/test';
const now = Date.parse('2026-10-25T12:00:00Z'), a = 'a'.repeat(32), b = 'b'.repeat(32);
const uuid = (i: number) => `00000000-0000-4000-8000-${String(i).padStart(12, '0')}`;
const iso = (offset = 0) => new Date(now + offset).toISOString();
const entry = (channelId = a) => ({ channelId, rank: 1, count: 0, referenceCount: 0, coverage: 1, status: 'HEALTHY', reasons: [], previousRank: null, previousReason: 'NO_PREVIOUS_SNAPSHOT' });
async function setup(page: Page) {
  await page.clock.install({ time: now });
  await page.route('**/api/channels**', route => {
    const url = new URL(route.request().url());
    const id = url.pathname.split('/')[3];
    const ids = url.searchParams.get('ids')?.split(',') ?? [a, b];
    const meta = (channelId: string) => ({ channelId, channelName: channelId === a ? '모험 채널' : '요리 채널', title: channelId === a ? '합성 모험 방송' : '합성 요리 방송', categoryId: 'game', categoryName: '게임', thumbnailUrl: null, liveStatus: 'LIVE', freshness: 'FRESH', reason: 'NONE', checkedAt: iso(), expiresAt: iso(60000) });
    return route.fulfill({ json: id ? meta(id) : { asOf: iso(), scope: 'COLLECTED_CHANNELS_ONLY', channels: ids.map(meta) } });
  });
  await page.route('**/api/v1/rankings?**', route => route.fulfill({ json: { asOf: iso(), evaluatedAt: iso(), snapshotId: uuid(1), windowSeconds: Number(new URL(route.request().url()).searchParams.get('window')), algorithmVersion: 'quality-ranking-v1', status: 'READY', entries: [entry()], excludedChannels: [{ ...entry(b), rank: null, count: null, referenceCount: 38, coverage: .8, status: 'INCOMPLETE', reasons: ['LOW_COVERAGE'] }] } }));
}
test('관측 정상 0건과 품질 불량 참고 건수를 구분하고 만료 시 순위를 내린다', async ({ page }) => {
  await setup(page); await page.goto('/explore');
  await expect(page.locator('.broadcast-card')).toHaveCount(2);
  await expect(page.locator('.broadcast-card').first().locator('.card-bottom strong')).toHaveText('0');
  await expect(page.locator('.broadcast-card').first()).toContainText('1위');
  await expect(page.locator('.broadcast-card').last().locator('.card-bottom strong')).toHaveText('—');
  await expect(page.locator('.broadcast-card').last()).toContainText('수집 불완전');
  await expect(page.locator('.broadcast-card').last()).toContainText('참고 수집 38건');
  await page.route('**/api/v1/rankings?**', route => route.fulfill({ status: 503, body: '' }));
  await page.clock.fastForward(30000);
  await expect(page.getByText('순위 갱신이 늦어지고 있어요.', { exact: false })).toBeVisible();
  await expect(page.locator('.broadcast-card').first().locator('.card-bottom strong')).toHaveText('—');
});
test('2배와 3배 리본·페이지 이동·품질 정정·오래된 분석을 구분한다', async ({ page }) => {
  await setup(page);
  const event = (id: number, channelId: string, count: number) => ({ spikeId: uuid(id), channelId, evaluatedAt: iso(-10000 * id), count, baseline: 50, ratio: count / 50, algorithmVersion: 'chat-spike-v1', valid: true, qualityWarning: null });
  await page.route('**/api/v1/spikes?**', route => {
    const next = Boolean(new URL(route.request().url()).searchParams.get('cursor'));
    return route.fulfill({ json: { asOf: iso(), status: 'READY', events: next ? [{ ...event(3, a, 100), valid: false, qualityWarning: 'COLLECTION_REVISED' }] : [event(1, a, 148), event(2, b, 150)], nextCursor: next ? null : `${iso(-20000)}|${uuid(2)}` } });
  });
  await page.goto('/surges');
  await expect(page.locator('.reaction-ribbon[data-level=rising]')).toHaveCount(1);
  await expect(page.locator('.reaction-ribbon[data-level=high]')).toHaveCount(1);
  await expect(page.locator('.reaction-ribbon[data-level=rising]')).toContainText('2.9');
  await page.getByRole('button', { name: '더 이전 반응' }).click();
  await expect(page.getByText('수집 공백이 확인되어 판정을 보류한 기록이에요.')).toBeVisible();
  await expect(page.locator('.reaction-ribbon')).toHaveCount(0);
  await page.getByRole('button', { name: '이전 페이지' }).click();
  await expect(page.locator('.reaction-ribbon')).toHaveCount(2);
  await page.route('**/api/v1/spikes?**', route => route.fulfill({ status: 503, body: '' }));
  await page.clock.fastForward(5000);
  await expect(page.locator('.reaction-ribbon')).toHaveCount(0);
});
test('상세 추이는 정상 0건과 공백을 따로 표시한다', async ({ page }) => {
  await setup(page);
  await page.route('**/api/v1/channels/*/activity?**', route => {
    const seconds = Number(new URL(route.request().url()).searchParams.get('seconds'));
    const start = now - seconds * 1000, from = new Date(start).toISOString(), to = iso();
    return route.fulfill({ json: { channelId: a, asOf: iso(), from, to, points: Array.from({ length: seconds / 60 }, (_, i) => ({
      from: new Date(start + i * 60000).toISOString(), to: new Date(start + (i + 1) * 60000).toISOString(),
      count: i === 0 ? 0 : null, referenceCount: i === 0 ? 0 : 5, coverage: i === 0 ? 1 : .5, status: i === 0 ? 'HEALTHY' : 'INCOMPLETE', reasons: i === 0 ? [] : ['LOW_COVERAGE'],
    })) } });
  });
  await page.goto(`/channels/${a}`);
  await expect(page.getByRole('table')).toBeVisible();
  await expect(page.locator('tbody strong')).toHaveCount(30);
  await expect(page.locator('tbody strong').last()).toHaveText('0');
  await expect(page.locator('tbody strong').first()).toHaveText('—');
  await expect(page.getByText('수집 불완전 · 참고 5건').first()).toBeVisible();
});
