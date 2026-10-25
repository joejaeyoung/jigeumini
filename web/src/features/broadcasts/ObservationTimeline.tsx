import { useState } from 'react';
import type { Observation, Timeline } from '../../shared/api/observations';
import { viewerSegments } from './observationModel';
import { formatCount, formatTime } from './model';

export function ObservationTimeline({ data }: { data: Timeline }) {
  const [selected, setSelected] = useState<Observation | null>(null);
  const from = Date.parse(data.from), to = Date.parse(data.to);
  const x = (time: string) => (Date.parse(time) - from) / (to - from) * 1000;
  const segments = viewerSegments(data.observations);
  const maxViewers = Math.max(1, ...data.observations.map(point => point.viewerCount ?? 0));
  const maxChat = Math.max(1, ...data.chat.map(point => point.count ?? 0));
  const y = (count: number, max: number) => 96 - count / max * 88;
  const active = selected && data.observations.find(point => point.eventId === selected.eventId);
  const unavailable = data.observations.filter(point => point.viewerCount === null).length;
  return <div className="observation-timeline" role="group" aria-label="같은 시간대의 시청자와 채팅 비교">
    <div className="chart-heading"><strong>시청자 관측 · 명</strong><span>축 최대 {formatCount(maxViewers)}명</span></div>
    {segments.length === 0 && <p className="observation-empty">이 시간대에는 시청자 관측값이 없어요.</p>}
    <svg className="observation-chart" viewBox="0 0 1000 100" preserveAspectRatio="none" role="group" aria-label="시청자 관측점 그래프">
      <line x1="0" x2="1000" y1="96" y2="96" className="chart-axis"/>
      {segments.map((segment, index) => <g key={index}>
        {segment.length > 1 && <polyline className="viewer-line" points={segment.map(point => `${x(point.observedAt)},${y(point.viewerCount!, maxViewers)}`).join(' ')}/>}
        {segment.map(point => <ellipse key={point.eventId} cx={x(point.observedAt)} cy={y(point.viewerCount!, maxViewers)} rx="5" ry="3" className="viewer-point"
          aria-label={`${formatTime(point.observedAt)} 시청자 ${formatCount(point.viewerCount!)}명`} onClick={() => setSelected(point)}>
          <title>{formatTime(point.observedAt)} · {formatCount(point.viewerCount!)}명</title>
        </ellipse>)}
      </g>)}
    </svg>
    <label className="observation-picker">시청자 관측값 선택
      <select value={active?.eventId ?? ''} onChange={event => setSelected(data.observations.find(point => point.eventId === event.target.value) ?? null)}>
        <option value="">관측 시각과 수치 선택</option>
        {data.observations.filter(point => point.viewerCount !== null).map(point => <option key={point.eventId} value={point.eventId}>{formatTime(point.observedAt)} · {formatCount(point.viewerCount!)}명</option>)}
      </select>
    </label>
    <p className="observation-selection" aria-live="polite">{active ? `${formatTime(active.observedAt)} 관측 · 시청자 ${formatCount(active.viewerCount!)}명` : '관측 시각을 선택하면 해당 수치를 볼 수 있어요.'}</p>
    <div className="chart-heading"><strong>완료된 1분 채팅 · 건</strong><span>축 최대 {formatCount(maxChat)}건</span></div>
    <svg className="observation-chart chat-chart" viewBox="0 0 1000 100" preserveAspectRatio="none" role="img" aria-label="완료된 1분 채팅 그래프. 음영은 수집 불완전 구간">
      <line x1="0" x2="1000" y1="96" y2="96" className="chart-axis"/>
      {data.chat.map(point => <rect key={point.from} x={x(point.from) + 1} width={Math.max(1, x(point.to) - x(point.from) - 2)}
        y={point.count === null ? 8 : y(point.count, maxChat)} height={point.count === null ? 88 : 96 - y(point.count, maxChat)}
        className={point.count === null ? 'chat-gap' : 'chat-bar'}><title>{formatTime(point.from)} · {point.count === null ? '수집 불완전' : `${formatCount(point.count)}건`}</title></rect>)}
    </svg>
    <div className="chart-times"><time dateTime={data.from}>{formatTime(data.from)}</time><time dateTime={new Date((from + to) / 2).toISOString()}>{formatTime(new Date((from + to) / 2).toISOString())}</time><time dateTime={data.to}>{formatTime(data.to)}</time></div>
    <p className="scope-note">시청자 표본은 기본 60초 간격입니다. 선은 관측점 사이의 변화만 보여주며, 누락·오래된 표본·방송 변경에서는 끊습니다. 채팅 음영은 품질 미확인 구간입니다.{unavailable > 0 && ` 수치 없는 관측 ${unavailable}회.`}</p>
    {data.truncated && <p className="notice">관측점이 많아 해당 기간의 최근 1,000개를 표시합니다.</p>}
  </div>;
}
