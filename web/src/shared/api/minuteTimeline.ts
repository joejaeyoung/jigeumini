const minute = 60_000;
type Interval = { from: string; to: string };

/** 서버가 반환한 완료 분 구간을 그대로 검증한다. 누락·중복을 정렬하거나 채우지 않는다. */
export function isMinuteTimeline(points: readonly Interval[], from: number, to: number): boolean {
  return from % minute === 0 && to % minute === 0 && points.length === (to - from) / minute
    && points.every((point, index) => Date.parse(point.from) === from + index * minute
      && Date.parse(point.to) === from + (index + 1) * minute);
}
