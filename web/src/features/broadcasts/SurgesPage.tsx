import { useState } from 'react';
import { fetchChannels } from '../../shared/api/client';
import { fetchSpikes } from '../../shared/api/quality';
import { usePolling } from '../../shared/api/usePolling';
import { currentMetadata, formatTime } from './model';
import { BroadcastCard } from './BroadcastCard';
import { Icon } from '../../shared/ui/Icon';
export function SurgesPage({ saved, toggle }: { saved: string[]; toggle: (id: string) => void }) {
  const [cursors, setCursors] = useState<string[]>(['']);
  const cursor = cursors.at(-1)!;
  const result = usePolling(`spikes:${cursor}`, signal => fetchSpikes(signal, cursor));
  const ids = [...new Set(result.data?.events.map(event => event.channelId) ?? [])];
  const metadata = usePolling(`spike-metadata:${ids.join(',')}`, signal => fetchChannels(signal, ids));
  const stale = result.error || Boolean(result.data && (result.data.status === 'STALE' || Date.now() >= Date.parse(result.data.asOf) + 30000));
  return <>
    <div className="page-heading"><div><h1>지금, 반응이 모이는 순간</h1><p>평소보다 반응이 늘어난 방송, 확인된 순간만 모았어요.</p></div><span className="scope-label">수집 대상 내 급상승</span></div>
    <p className="surge-explainer">최근 1분과 앞선 5분의 평균을 비교해요. 수집 품질을 확인하고 10초 간격으로 두 번 연속 조건을 만족하면 기록해요.</p>
    {result.error && <div className="notice error" role="alert">급상승을 갱신하지 못했어요.<button onClick={result.retry}>다시 시도</button></div>}
    {stale && <p className="notice" role="status">급상승 분석이 지연되고 있어요. 아래는 이전 기록이며 반응 리본을 보류합니다.</p>}
    {metadata.error && <p className="notice" role="status">방송 제목과 이미지를 확인하지 못했어요.</p>}
    {result.loading && !result.data && <p className="loading-state" role="status">확인된 반응을 불러오는 중…</p>}
    {result.data && (result.data.events.length ? <ul className="broadcast-grid surge-grid">{result.data.events.map(event => <li className="surge-item" key={event.spikeId}>
      <ul><BroadcastCard channel={{ id: event.channelId, chatCount: event.count, lastBucket: event.evaluatedAt, metadata: currentMetadata(metadata.data?.find(meta => meta.channelId === event.channelId)) }} seconds={60} saved={saved.includes(event.channelId)} onSave={toggle} recorded reaction={event.valid && !stale ? event.ratio : undefined}/></ul>
      <p className="surge-context"><time dateTime={event.evaluatedAt}>{formatTime(event.evaluatedAt)}</time> 기록 · 이전 1분 평균 {event.baseline.toLocaleString('ko-KR', { maximumFractionDigits: 1 })}건</p>
      {!event.valid && <p className="quality-caption">수집 공백이 확인되어 판정을 보류한 기록이에요.</p>}
    </li>)}</ul> : <section className="empty-state"><Icon name="trend"/><h2>{result.data.status === 'WARMING_UP' ? '반응을 비교할 기록을 모으고 있어요' : '아직 확인된 급상승이 없어요'}</h2><p>최소 6분의 정상 관측과 연속 조건이 필요해요.<br/>집계가 부족한 방송의 배수를 임의로 표시하지 않아요.</p></section>)}
    <div className="feed-pagination">{cursors.length > 1 && <button className="secondary-button" onClick={() => setCursors(previous => previous.slice(0, -1))}>이전 페이지</button>}{result.data?.nextCursor && <button className="secondary-button" onClick={() => setCursors(previous => [...previous, result.data!.nextCursor!])}>더 이전 반응</button>}</div>
  </>;
}
