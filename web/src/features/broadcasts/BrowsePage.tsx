import { Link, useSearchParams } from 'react-router';
import { fetchSnapshot } from '../../shared/api/client';
import { usePolling } from '../../shared/api/usePolling';
import { summarize, withSavedChannels, formatTime } from './model';
import { BroadcastCard } from './BroadcastCard';
import { Icon } from '../../shared/ui/Icon';
export type BrowseView = 'home' | 'explore' | 'saved' | 'search';
const copy: Record<BrowseView, { title: string; description: string }> = {
  home: { title: '오늘의 발견', description: '채팅이 모이는 순간부터, 새로운 방송을 만나보세요.' },
  explore: { title: '방송 탐색', description: '수집한 방송의 채팅 반응을 한눈에 살펴보세요.' },
  saved: { title: '다시 만나고 싶은 방송', description: '눈여겨본 채널을 한곳에 모아두세요.' },
  search: { title: '방송 검색', description: '수집한 채널의 ID로 검색할 수 있어요.' },
};
export function BrowsePage({ view, saved, toggle }: { view: BrowseView; saved: string[]; toggle: (id: string) => void }) {
  const [params, setParams] = useSearchParams();
  const seconds = params.get('window') === '60' ? 60 : 300;
  const query = view === 'search' ? (params.get('q') ?? '').trim().toLowerCase() : '';
  const result = usePolling(`browse:${seconds}`, signal => fetchSnapshot(seconds, signal));
  const summaries = result.data ? summarize(result.data.counts.buckets) : [];
  const channels = (view === 'saved' ? withSavedChannels(summaries, saved) : summaries).filter(channel => channel.id.includes(query));
  const consumer = result.data?.status.consumer;
  const healthy = consumer?.running && consumer.assignedPartitions > 0 && ['RUNNING', 'WAITING_FOR_EVENTS'].includes(consumer.state);
  const showCards = Boolean(result.data) || (view === 'saved' && saved.length === 0);
  function changeWindow(value: number) {
    const next = new URLSearchParams(params); next.set('window', String(value)); setParams(next, { replace: true, preventScrollReset: true });
  }
  return <>
    <div className="page-heading"><div><h1>{copy[view].title}</h1><p>{copy[view].description}</p></div><span className="scope-label">수집 대상 내 채팅 반응</span></div>
    {view === 'home' && <section className="discovery-hero" aria-labelledby="hero-heading"><div className="hero-art" role="img" aria-label="초록빛 숲과 돌문을 그린 서비스 일러스트"><span className="image-caption">서비스 일러스트 · AI 제작</span></div><div className="hero-copy"><span className="eyebrow">다음 방송을 만나는 방법</span><h2 id="hero-heading">채팅이 모이는 순간,<br/>어떤 방송일까요?</h2><p>지금 수집된 반응을 따라<br/>새로운 방송을 발견해보세요.</p><Link to="/explore" className="primary-button">방송 둘러보기 <Icon name="arrow"/></Link></div></section>}
    <section aria-labelledby="list-heading"><div className="section-heading"><div><h2 id="list-heading">{view === 'saved' ? '내가 저장한 채널' : view === 'search' ? '검색 결과' : '수집된 방송 둘러보기'}</h2><p>{query ? `“${query}” 검색 결과예요.` : '최근 수집된 채팅 건수 순서예요.'}</p></div><div className="segments" role="group" aria-label="조회 기간">{[60, 300].map(value => <button key={value} aria-pressed={seconds === value} onClick={() => changeWindow(value)}>최근 {value / 60}분</button>)}</div></div>
      <div className="list-tools"><span className="category-tag">전체</span><span>{showCards ? `${channels.length}개 채널` : '조회 중'}</span></div>
      {result.error && <div className="notice error" role="alert"><span>{result.data ? '갱신하지 못했어요. 아래 내용은 이전 조회 결과입니다.' : '방송 정보를 불러오지 못했어요. 잠시 후 다시 시도해주세요.'}</span><button onClick={result.retry}>다시 시도</button></div>}
      {result.data && !healthy && <div className="notice" role="status">새 채팅 반영 상태를 확인하고 있어요. 표시된 건수는 최신 반응과 다를 수 있어요.</div>}
      {result.loading && !result.data && <div className="loading-state" role="status"><Icon name="broadcast"/><p>수집된 방송을 불러오는 중…</p></div>}
      {showCards && (channels.length ? <ul className="broadcast-grid">{channels.map(channel => <BroadcastCard key={channel.id} channel={channel} seconds={seconds} saved={saved.includes(channel.id)} onSave={toggle}/>)}</ul> : <div className="empty-state"><Icon name={view === 'saved' ? 'bookmark' : 'broadcast'}/><h3>{view === 'saved' ? '다시 보고 싶은 방송을 저장해보세요' : query ? '검색한 채널을 찾지 못했어요' : '아직 이 시간대에 수집된 방송이 없어요'}</h3><p>{view === 'saved' ? '방송 카드의 북마크를 누르면 이곳에서 다시 찾을 수 있어요.' : query ? '채널 ID를 다시 확인하거나 전체 방송을 둘러보세요.' : '새 기록이 들어오면 자동으로 보여드릴게요. 기록 없음은 채팅 0건을 뜻하지 않아요.'}</p>{view === 'saved' || query ? <Link className="primary-button" to="/explore">방송 둘러보기 <Icon name="arrow"/></Link> : <button className="secondary-button" onClick={result.retry}>다시 확인</button>}</div>)}
      {result.data && <div className="scope-note"><span>조회 <time dateTime={result.data.counts.asOf}>{formatTime(result.data.counts.asOf)}</time> · 5초마다 갱신</span><span>진행 중인 구간 {result.data.counts.includesOpenBucket ? '포함' : '제외'} · 수집 공백 미측정</span></div>}
    </section>
  </>;
}
