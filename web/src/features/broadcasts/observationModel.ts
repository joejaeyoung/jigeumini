import type { Observation } from '../../shared/api/observations';

/** 관측점 사이만 연결한다. 실패 표본·TTL 초과·방송 변경에서는 연결하지 않는다. */
export function viewerSegments(samples: Observation[]): Observation[][] {
  const segments: Observation[][] = [];
  let previous: Observation | undefined;
  for (const sample of samples) {
    if (sample.quality !== 'OBSERVED' || sample.viewerCount === null) { previous = undefined; continue; }
    if (!previous || Date.parse(sample.observedAt) - Date.parse(previous.observedAt) > Math.min(previous.ttlSeconds, sample.ttlSeconds) * 1000
      || previous.sourceBroadcastId !== sample.sourceBroadcastId) segments.push([]);
    segments[segments.length - 1].push(sample);
    previous = sample;
  }
  return segments;
}
