import { MinuteBars } from '../../shared/ui/MinuteBars';
import { Link, useParams } from 'react-router';
import { fetchCategories, fetchCategoryTimeline, type CategoryEntry, type CategoryTimeline } from '../../shared/api/categories';
import { usePolling } from '../../shared/api/usePolling';
import { fetchChannels } from '../../shared/api/metadata';

const number = (value: number | null) => value === null ? '—' : value.toLocaleString('ko-KR');
const time = (value: string) => new Date(value).toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit', hour12: false });
export function CategoriesPage() {
  const { categoryKey } = useParams();
  const result = usePolling('categories', fetchCategories);
  const selected = result.data?.categories.find(c => c.categoryKey === categoryKey);
  return <section className="categories-page">
    <div className="page-heading"><div><p className="eyebrow">수집 대상 내 반응</p><h1>{categoryKey ? selected?.name ?? '카테고리 상세' : '카테고리 순위'}</h1></div></div>
    <p className="section-description">완료된 5분의 채팅을 당시 관측한 카테고리로 묶어요. 카테고리는 최대 120초 동안 유효하며 실제 전환 시각과 다를 수 있어요.</p>
    {result.error && <p className="notice" role="alert">카테고리를 불러오지 못했어요. {result.data && '이전 조회 결과예요.'} <button onClick={result.retry}>다시 시도</button></p>}
    {result.loading && !result.data && <p role="status">카테고리 반응을 확인하고 있어요.</p>}
    {result.data && <>
      <p className="category-context">{time(result.data.from)}–{time(result.data.to)} · 수집 대상 내 비중 · 미상 참고 {number(result.data.unknownReferenceCount)}건 · 정정 버전 {result.data.revision}</p>
      {result.data.status === 'LAGGING' && <p className="notice">카테고리 분석의 진행을 확인하지 못했어요. 건수와 비중은 확인 후 표시해요.</p>}
      {result.data.truncated && <p className="notice">최근 관측한 20개 채널만 표시해요.</p>}
      {!categoryKey && (result.data.categories.length ? <div className="category-list">{result.data.categories.map((entry, index) => <article className="category-row" key={entry.categoryKey}>
        <div><span className="category-rank">{entry.count === null ? '—' : index + 1}</span><Link to={`/categories/${entry.categoryKey}`}>{entry.name}</Link></div>
        <div><strong>{number(entry.count)}<small> 건</small></strong><span>품질 충족 {entry.healthyChannels} / 관측 {entry.observedChannels}채널</span></div>
        <div><strong>{entry.share === null ? '—' : `${(entry.share * 100).toFixed(1)}%`}</strong><span>참고 {number(entry.referenceCount)}건</span></div>
      </article>)}</div> : <div className="empty-state"><h2>아직 카테고리 관측이 없어요</h2><p>수집한 채널의 기록이 쌓이면 반응을 비교할 수 있어요.</p></div>)}
    </>}
    {categoryKey && <><Link className="category-back" to="/categories">← 카테고리 순위</Link><CategoryDetail categoryKey={categoryKey} entry={selected}/></>}
  </section>;
}
function CategoryDetail({ categoryKey, entry }: { categoryKey: string; entry?: CategoryEntry }) {
  const result = usePolling(`category:${categoryKey}`, signal => fetchCategoryTimeline(categoryKey, signal));
  const ids = entry?.channels.map(channel => channel.channelId) ?? [];
  const metadata = usePolling(`category-channels:${ids.join(',')}`, signal => fetchChannels(signal, ids), 30000);
  return <>
    {result.error && <p className="notice" role="alert">시간대 추이를 불러오지 못했어요. {result.data && '이전 조회 결과예요.'} <button onClick={result.retry}>다시 시도</button></p>}
    {result.loading && !result.data && <p role="status">최근 1시간을 확인하고 있어요.</p>}
    {result.data && <CategoryChart timeline={result.data}/>}
    <h2>최근 5분 기여 채널</h2>
    {entry?.channels.length ? <ul className="category-contributors">{entry.channels.map((channel, index) => <li key={channel.channelId}><Link to={`/channels/${channel.channelId}`}>{metadata.data?.find(item => item.channelId === channel.channelId)?.channelName ?? `기여 방송 ${index + 1}`}</Link><strong>{number(channel.count)}건</strong><span>{channel.quality === 'HEALTHY' ? '품질 충족' : `품질 미확인 · 참고 ${number(channel.referenceCount)}건`}</span></li>)}</ul> : <p>이 구간에는 기여 채널 기록이 없어요.</p>}
  </>;
}
function CategoryChart({ timeline }: { timeline: CategoryTimeline }) {
  const max = Math.max(1, ...timeline.points.map(p => p.count ?? 0));
  return <section className="category-chart" aria-label="최근 1시간 카테고리 채팅">
    <h2>최근 1시간</h2><p>분당 채팅(건) · 최대 {number(max)} · 정정 버전 {timeline.revision}</p>
    {timeline.status === 'LAGGING' && <p className="notice">분석 진행 미확인 · 건수를 보류했어요.</p>}
    {timeline.truncated && <p className="notice">최근 관측한 20개 채널 범위예요.</p>}
    <svg viewBox="0 0 600 150" role="img" aria-label="분당 채팅 막대그래프. 공백은 음영으로 표시하며 수치는 아래 표에서 확인할 수 있어요.">
      <MinuteBars points={timeline.points} formatTime={time}/>
      <line x1="0" y1="140" x2="600" y2="140" stroke="currentColor"/>
    </svg><div className="category-axis"><span>{time(timeline.from)}</span><span>{time(timeline.to)}</span></div>
    <p>음영은 미확인 구간이에요. 정상 관측된 0건과 구분해요.</p>
    <details><summary>분별 건수와 품질 보기</summary><div className="category-table"><table><thead><tr><th>시각</th><th>채팅</th><th>참고</th><th>품질 충족 / 관측</th></tr></thead><tbody>{timeline.points.map(p => <tr key={p.from}><td>{time(p.from)}</td><td>{number(p.count)}</td><td>{number(p.referenceCount)}</td><td>{p.healthyChannels} / {p.observedChannels}</td></tr>)}</tbody></table></div></details>
  </section>;
}
