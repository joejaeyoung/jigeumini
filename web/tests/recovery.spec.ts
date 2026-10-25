import { expect, test } from '@playwright/test';
import { readFileSync } from 'node:fs';

const html = readFileSync(new URL('../../server/src/main/resources/static/index.html', import.meta.url), 'utf8');
const at = '2026-10-25T12:00:00Z';
test('운영 진단은 정상 0 lag·미관측·격리·보존 만료와 조회 실패를 구분한다', async ({ page }) => {
  await page.clock.install({ time: Date.parse(at) });
  await page.route('**/operations-test', route => route.fulfill({ contentType: 'text/html', body: html }));
  await page.route('**/api/status', route => route.fulfill({ json: { asOf: at, totals: { processedEvents: 2, bucketTotal: 2 }, consumer: { state: 'RUNNING', running: true, assignedPartitions: 3 } } }));
  await page.route('**/api/buckets?**', route => route.fulfill({ json: { buckets: [] } }));
  const group = {
    group: 'synthetic-ranking', projection: 'ranking', consumer: { state: 'RUNNING', failedAttempts: 2, lastErrorCode: 'NONE' },
    partitions: [
      { partition: 0, lag: 0, committedOffset: 3, dbResolvedOffset: 3, firstPendingAt: null, lastProgressAt: at, state: 'CAUGHT_UP' },
      { partition: 1, lag: null, committedOffset: null, dbResolvedOffset: null, firstPendingAt: null, lastProgressAt: null, state: 'OFFSET_UNSET' },
    ],
    errors: [{ partition: 0, errorCode: 'INVALID_EVENT_JSON', count: 1 }],
    recentFailures: [{ failureId: 'a'.repeat(64), partition: 0, offset: 1, errorCode: 'INVALID_EVENT_JSON', quarantinedAt: at, sourceAvailability: 'EXPIRED' }],
  };
  let unavailable = false;
  await page.route('**/api/operations/recovery', route => route.fulfill({ json: { observedAt: at, state: unavailable ? 'UNAVAILABLE' : 'AVAILABLE', groups: [group] } }));
  await page.goto('/operations-test');
  const section = page.getByRole('region', { name: '소비 복구 진단' });
  await expect(section.getByRole('heading', { name: 'synthetic-ranking' })).toBeVisible();
  await expect(section.locator('tbody tr').first().locator('td').nth(1)).toHaveText('0');
  await expect(section.locator('tbody tr').last().locator('td').nth(1)).toHaveText('—');
  await expect(section).toContainText('INVALID_EVENT_JSON 1건');
  await section.locator('summary').click();
  await expect(section).toContainText('원본 보존 만료 · 복구 불가');
  unavailable = true; await page.clock.fastForward(5000);
  await expect(section.getByRole('status')).toContainText('현재 관측 불가');
  await expect(section).toContainText('이전 결과');
  await page.route('**/api/operations/recovery', route => route.fulfill({ status: 503 }));
  await page.clock.fastForward(5000);
  await expect(section.getByRole('status')).toContainText('진단 조회 실패');
  await expect(section.locator('tbody tr')).toHaveCount(2);
});
