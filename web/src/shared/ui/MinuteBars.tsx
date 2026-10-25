type Point = { from: string; count: number | null };

/** null은 구간 전체 음영, 관측된 0은 높이 0으로 표현한다. SVG 크기·축·표는 화면이 소유한다. */
export function MinuteBars({ points, formatTime }: { points: readonly Point[]; formatTime: (value: string) => string }) {
  const maximum = Math.max(1, ...points.map(point => point.count ?? 0));
  return <>{points.map((point, index) => <rect key={point.from} x={index * 10 + 1} width="8"
    y={point.count === null ? 0 : 140 - point.count / maximum * 130}
    height={point.count === null ? 140 : point.count / maximum * 130}
    className={point.count === null ? 'category-gap' : 'category-bar'}>
    <title>{formatTime(point.from)} {point.count === null ? '미확인' : `${point.count.toLocaleString('ko-KR')}건`}</title>
  </rect>)}</>;
}
