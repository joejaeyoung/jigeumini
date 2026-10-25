import { useState } from 'react';
import { Link, useParams } from 'react-router';
import { channelIdSchema, fetchBuckets } from '../../shared/api/client';
import { usePolling } from '../../shared/api/usePolling';
import { channelName, formatCount, formatTime } from './model';
import { Icon } from '../../shared/ui/Icon';
export function ChannelPage({ saved, toggle }: { saved: string[]; toggle: (id: string) => void }) {
  const { channelId = '' } = useParams();
  if (!channelIdSchema.safeParse(channelId).success) return <section className="empty-state"><h1>채널을 찾을 수 없어요</h1><Link to="/explore">방송 탐색으로 돌아가기</Link></section>;
  return <ChannelDetail key={channelId} id={channelId} saved={saved.includes(channelId)} toggle={toggle}/>;
}
function ChannelDetail({ id, saved, toggle }: { id: string; saved: boolean; toggle: (id: string) => void }) {
  const [seconds, setSeconds] = useState(1800);
  const result = usePolling(`channel:${id}:${seconds}`, signal => fetchBuckets(seconds, signal, id));
  const rows = [...(result.data?.buckets ?? [])].sort((a, b) => b.bucketStart.localeCompare(a.bucketStart));
  const max = Math.max(1, ...rows.map(row => row.chatCount));
  return <>
    <Link className="back-link" to="/explore">← 방송 탐색</Link>
    <div className="page-heading"><div><h1>{channelName(id)}</h1><p>채팅이 수집된 구간의 기록을 살펴보세요.</p></div><button className="secondary-button" aria-pressed={saved} onClick={() => toggle(id)}><Icon name="bookmark"/>{saved ? '관심 방송 해제' : '관심 방송 저장'}</button></div>
    <section className="channel-intro"><span className="channel-symbol"><Icon name="broadcast"/></span><div><h2>어떤 방송인지 궁금하다면</h2><p>방송 제목과 이미지는 준비 중이에요. 현재 방송 여부는 치지직에서 확인할 수 있어요.</p><a className="primary-button" href={`https://chzzk.naver.com/live/${id}`} target="_blank" rel="noopener noreferrer">치지직 채널 보기 <span aria-hidden="true">↗</span></a></div></section>
    <section aria-labelledby="history-heading"><div className="section-heading"><div><h2 id="history-heading">수집된 채팅 기록</h2><p>기록이 없는 구간은 0건으로 채우지 않아요.</p></div><div className="segments" role="group" aria-label="추이 조회 기간">{[1800, 3600].map(value => <button key={value} aria-pressed={seconds === value} onClick={() => setSeconds(value)}>{value / 60}분</button>)}</div></div>
      {result.error && <div className="notice error" role="alert">{result.data ? '갱신하지 못했어요. 이전 조회 결과입니다.' : '기록을 불러오지 못했어요.'}<button onClick={result.retry}>다시 시도</button></div>}
      {result.loading && !result.data && <p role="status" className="loading-state">채팅 기록을 불러오는 중…</p>}
      {result.data && (rows.length ? <div className="history-table"><table><caption>최근 {seconds / 60}분 · 10초 구간별 수집 건수</caption><thead><tr><th scope="col">구간 시작</th><th scope="col">채팅 수</th></tr></thead><tbody>{rows.map(row => <tr key={row.bucketStart}><td><time dateTime={row.bucketStart}>{formatTime(row.bucketStart)}</time></td><td><div className="count-bar"><span style={{ width: `${row.chatCount / max * 100}%` }}/><strong>{formatCount(row.chatCount)}</strong></div></td></tr>)}</tbody></table></div> : <div className="empty-state"><h3>이 시간대에는 수집된 기록이 없어요</h3><p>방송 종료나 채팅 0건을 뜻하지는 않아요.</p></div>)}
      {result.data && <p className="scope-note">조회 {formatTime(result.data.asOf)} · 진행 중인 구간 {result.data.includesOpenBucket ? '포함' : '제외'} · 수집 공백 미측정</p>}
    </section>
  </>;
}
