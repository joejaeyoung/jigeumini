import { useState } from 'react';
import { Link } from 'react-router';
import { canHighlight, fetchNotifications } from '../../shared/api/notifications';
import { fetchChannels } from '../../shared/api/metadata';
import { usePolling } from '../../shared/api/usePolling';
import { currentMetadata } from '../broadcasts/model';
import { useReadNotifications } from './useReadNotifications';
export function NotificationsPage({ saved }: { saved: string[] }) {
  const [selected, setSelected] = useState('');
  const { read, markRead, storageError } = useReadNotifications();
  const channels = selected && saved.includes(selected) ? [selected] : saved.slice(0, 20).sort();
  const filter = channels.join(',');
  return <NotificationFeed key={filter} channels={channels} saved={saved} selected={selected} setSelected={setSelected} read={read} markRead={markRead} storageError={storageError}/>;
}
function NotificationFeed({ channels, saved, selected, setSelected, read, markRead, storageError }: {
  channels: string[]; saved: string[]; selected: string; setSelected: (id: string) => void;
  read: Record<string, number>; markRead: (id: string) => void; storageError: boolean;
}) {
  const [cursors, setCursors] = useState<string[]>(['']);
  const cursor = cursors.at(-1)!;
  const result = usePolling(`notifications:${channels.join(',')}:${cursor}`, signal => fetchNotifications(channels, signal, cursor));
  const metadata = usePolling(`notification-metadata:${channels.join(',')}`, signal => fetchChannels(signal, channels));
  const label = (id: string) => currentMetadata(metadata.data?.find(meta => meta.channelId === id))?.channelName ?? `방송 ${id.slice(0, 8)}`;
  const stale = result.error || Boolean(result.data && Date.now() >= Date.parse(result.data.asOf) + 30000);
  return <>
    <div className="page-heading"><div><h1>관심 방송 알림</h1><p>저장한 방송의 반응이 늘어난 순간을 모았어요.</p></div><Link className="secondary-button" to="/saved">관심 방송 관리</Link></div>
    <p className="surge-explainer">최근 7일의 급상승 기록이에요. 10분 넘게 늦게 도착했거나 수집 불완전이 확인된 항목은 새 반응으로 강조하지 않아요. 읽음 표시는 이 브라우저에 저장돼요.</p>
    {saved.length > 20 && <p className="notice">한 번에 20개 관심 방송을 보여줘요. 특정 방송을 선택하면 다른 관심 방송도 확인할 수 있어요.</p>}
    {saved.length > 0 && <label className="notification-filter">알림 받을 방송<select aria-label="관심 채널 필터" value={selected} onChange={event => setSelected(event.target.value)}><option value="">관심 방송 전체</option>{saved.map(id => <option key={id} value={id}>{label(id)}</option>)}</select></label>}
    {storageError && <p className="notice" role="alert">저장 공간을 사용할 수 없어 읽음 표시는 현재 화면에서만 유지돼요.</p>}
    {result.error && <div className="notice error" role="alert">알림을 갱신하지 못했어요. 이전 기록을 표시해요.<button onClick={result.retry}>다시 시도</button></div>}
    {result.loading && !result.data && <p className="loading-state" role="status">알림을 불러오는 중…</p>}
    {!saved.length ? <section className="empty-state"><h2>관심 방송을 먼저 저장해주세요</h2><Link to="/explore" className="primary-button">방송 둘러보기</Link></section> : result.data && !result.data.events.length ? <section className="empty-state"><h2>아직 도착한 알림이 없어요</h2><p>관심 방송의 확인된 급상승이 도착하면 표시해요.</p></section> : <ul className="notification-list">{result.data?.events.map(event => {
      const fresh = !read[event.spikeId] && !stale && canHighlight(event, Date.now());
      return <li key={event.spikeId} className={fresh ? 'notification-item is-new' : 'notification-item'}>
        <div className="notification-content"><div className="notification-meta"><time dateTime={event.evaluatedAt}>{new Date(event.evaluatedAt).toLocaleString('ko-KR', { month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit' })}</time><span>{event.delayed ? '늦게 도착한 기록' : read[event.spikeId] ? '읽음' : fresh ? '새 반응' : '이전 기록'}</span></div>
        <h2><Link to={`/channels/${event.channelId}`} onClick={() => markRead(event.spikeId)}>{label(event.channelId)}</Link></h2>
        <p>1분 채팅 {event.count.toLocaleString('ko-KR')}건 · 이전 평균 {event.baseline.toLocaleString('ko-KR')}건</p>
        {!event.valid && <p className="quality-caption">수집 불완전이 확인되어 급상승 판정을 보류했어요.</p>}</div>
        <button className="secondary-button" disabled={Boolean(read[event.spikeId])} onClick={() => markRead(event.spikeId)}>{read[event.spikeId] ? '읽음' : '읽음으로 표시'}</button>
      </li>;
    })}</ul>}
    <div className="feed-pagination">{cursors.length > 1 && <button className="secondary-button" onClick={() => setCursors(cursors.slice(0, -1))}>이전 페이지</button>}{result.data?.nextCursor && <button className="secondary-button" onClick={() => setCursors([...cursors, result.data!.nextCursor!])}>더 이전 알림</button>}</div>
  </>;
}
