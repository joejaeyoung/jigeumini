import { expect, it } from 'vitest';
import type { Observation } from '../../shared/api/observations';
import { viewerSegments } from './observationModel';
const id = 'a'.repeat(32), end = Date.parse('2026-10-25T12:00:00Z'), start = end - 3600000;
const iso = (millis: number) => new Date(millis).toISOString();
function sample(index: number, offset: number, count: number | null = 10): Observation {
  return { schemaVersion: 1, eventId: `00000000-0000-4000-8000-${String(index).padStart(12, '0')}`, runId: 'test-v1', channelId: id,
    observedAt: iso(start + offset * 1000), ttlSeconds: 120, viewerCount: count, liveStatus: 'LIVE', quality: count === null ? 'MISSING_VIEWER_COUNT' : 'OBSERVED',
    sourceBroadcastId: '123', broadcastIdentity: 'SOURCE_PROVIDED', categoryId: null, categoryName: null };
}
it('실패·TTL 초과·방송 전환에 선을 끊고 관측 시각을 옮기거나 연장하지 않는다', () => {
  const points = [sample(1, 7, 0), sample(2, 67), sample(3, 127, null), sample(4, 187), sample(5, 307), sample(6, 428), { ...sample(7, 488), sourceBroadcastId: '456' }];
  const segments = viewerSegments(points);
  expect(segments.map(segment => segment.map(point => point.eventId))).toEqual([[points[0].eventId, points[1].eventId], [points[3].eventId, points[4].eventId], [points[5].eventId], [points[6].eventId]]);
  expect(segments[0][0].observedAt).toBe(iso(start + 7000));
});
