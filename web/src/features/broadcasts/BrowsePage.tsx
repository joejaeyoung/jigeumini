import { Link, useSearchParams } from 'react-router';
import { fetchChannels } from '../../shared/api/client';
import { fetchRankings, rankingStale } from '../../shared/api/quality';
import { usePolling } from '../../shared/api/usePolling';
import { rankedChannels, withSavedChannels, withMetadata, matchesSearch, formatTime } from './model';
import { BroadcastCard } from './BroadcastCard';
import { Icon } from '../../shared/ui/Icon';
export type BrowseView = 'home' | 'explore' | 'saved' | 'search';
const copy: Record<BrowseView, { title: string; description: string }> = {
  home: { title: '오늘의 발견', description: '채팅이 모이는 순간부터, 새로운 방송을 만나보세요.' },
  explore: { title: '방송 탐색', description: '수집한 방송의 채팅 반응을 한눈에 살펴보세요.' },
  saved: { title: '다시 만나고 싶은 방송', description: '눈여겨본 채널을 한곳에 모아두세요.' },
  search: { title: '방송 검색', description: '방송 제목, 채널명, 카테고리로 검색해보세요.' },
};
export function BrowsePage({ view, saved, toggle }: { view: BrowseView; saved: string[]; toggle: (id: string) => void }) {
  const [params, setParams] = useSearchParams();
  const seconds = params.get('window') === '60' ? 60 : 300;
  const query = view === 'search' ? (params.get('q') ?? '').trim().toLowerCase() : '';
  const result = usePolling(`browse:${seconds}`, signal => fetchRankings(seconds, signal));
  const metadata = usePolling(`metadata:${view === 'saved' ? saved.join(',') : 'directory'}`, signal => fetchChannels(signal, view === 'saved' ? saved : undefined));
  const summaries = withMetadata(result.data ? rankedChannels(result.data) : [], metadata.data ?? []);
  const source = view === 'saved' ? withSavedChannels(summaries, saved) : summaries;
  const category = params.get('category') ?? '';
  const categories = [...new Set(source.flatMap(channel => channel.metadata?.categoryName ? [channel.metadata.categoryName] : []))].sort();
  const channels = source.filter(channel => matchesSearch(channel, query) && (!category || channel.metadata?.categoryName === category));
  const stale = result.data ? rankingStale(result.data) : false;
  const showCards = Boolean(result.data) || Boolean(metadata.data?.length) || (view === 'saved' && saved.length === 0);
  function changeWindow(value: number) {
    const next = new URLSearchParams(params); next.set('window', String(value)); setParams(next, { replace: true, preventScrollReset: true });
  }
  function changeCategory(value: string) {
    const next = new URLSearchParams(params); if (value) next.set('category', value); else next.delete('category');
    setParams(next, { replace: true, preventScrollReset: true });
  }
  return <>
    <div className="page-heading"><div><h1>{copy[view].title}</h1><p>{copy[view].description}</p></div><span className="scope-label">수집 대상 내 채팅 반응</span></div>
    {view === 'home' && <section className="discovery-hero" aria-labelledby="hero-heading"><div className="hero-art" role="img" aria-label="초록빛 숲과 돌문을 그린 서비스 일러스트"><span className="image-caption">서비스 일러스트 · AI 제작</span></div><div className="hero-copy"><span className="eyebrow">다음 방송을 만나는 방법</span><h2 id="hero-heading">채팅이 모이는 순간,<br/>어떤 방송일까요?</h2><p>지금 수집된 반응을 따라<br/>새로운 방송을 발견해보세요.</p><Link to="/explore" className="primary-button">방송 둘러보기 <Icon name="arrow"/></Link></div></section>}
    <section aria-labelledby="list-heading"><div className="section-heading"><div><h2 id="list-heading">{view === 'saved' ? '내가 저장한 채널' : view === 'search' ? '검색 결과' : '수집된 방송 둘러보기'}</h2><p>{query ? `“${query}” 검색 결과예요.` : '완료된 구간의 채팅 순위 · 수집 품질을 확인한 방송부터 보여요.'}</p></div><div className="segments" role="group" aria-label="조회 기간">{[60, 300].map(value => <button key={value} aria-pressed={seconds === value} onClick={() => changeWindow(value)}>최근 {value / 60}분</button>)}</div></div>
      <div className="list-tools"><div className="category-filters" role="group" aria-label="방송 카테고리"><button className="category-tag" aria-pressed={!category} onClick={() => changeCategory('')}>전체</button>{[...new Set([...categories, ...(category ? [category] : [])])].map(name => <button key={name} className="category-tag" aria-pressed={category === name} onClick={() => changeCategory(name)}>{name}</button>)}</div><span>{showCards ? `${channels.length}개 채널` : '조회 중'}</span></div>
      {result.error && <div className="notice error" role="alert"><span>{result.data ? '갱신하지 못했어요. 아래 내용은 이전 조회 결과입니다.' : '채팅 순위를 불러오지 못했어요. 잠시 후 다시 시도해주세요.'}</span><button onClick={result.retry}>다시 시도</button></div>}
      {metadata.error && <div className="notice" role="status"><span>방송 제목과 이미지를 갱신하지 못했어요. 수집된 채팅 건수는 별도로 표시해요.</span><button onClick={metadata.retry}>방송 정보 재시도</button></div>}
      {stale && <div className="notice" role="status">순위 갱신이 늦어지고 있어요. 이전 결과의 순위와 건수는 보류합니다.</div>}
      {result.data?.status === 'WARMING_UP' && <div className="notice" role="status">수집 품질을 확인하고 있어요. 정상 관측 구간이 쌓이면 순위에 표시돼요.</div>}
      {result.loading && !result.data && <div className="loading-state" role="status"><Icon name="broadcast"/><p>수집된 방송을 불러오는 중…</p></div>}
      {showCards && (channels.length ? <ul className="broadcast-grid">{channels.map(channel => <BroadcastCard key={channel.id} channel={channel} countsKnown={Boolean(result.data)} seconds={seconds} saved={saved.includes(channel.id)} onSave={toggle}/>)}</ul> : <div className="empty-state"><Icon name={view === 'saved' ? 'bookmark' : 'broadcast'}/><h3>{view === 'saved' ? '다시 보고 싶은 방송을 저장해보세요' : query || category ? '검색한 채널을 찾지 못했어요' : '아직 이 시간대에 수집된 방송이 없어요'}</h3><p>{view === 'saved' ? '방송 카드의 북마크를 누르면 이곳에서 다시 찾을 수 있어요.' : query || category ? '검색어나 카테고리를 바꾸거나 전체 방송을 둘러보세요.' : '새 기록이 들어오면 자동으로 보여드릴게요. 기록 없음은 채팅 0건을 뜻하지 않아요.'}</p>{view === 'saved' || query || category ? <Link className="primary-button" to="/explore">방송 둘러보기 <Icon name="arrow"/></Link> : <button className="secondary-button" onClick={result.retry}>다시 확인</button>}</div>)}
      {result.data && <div className="scope-note"><span>조회 <time dateTime={result.data.asOf}>{formatTime(result.data.asOf)}</time> · 5초마다 갱신</span><span>완료된 {seconds / 60}분 · 연결 관측 95% 이상만 순위 포함</span></div>}
    </section>
  </>;
}
