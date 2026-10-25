import { Link, useParams } from 'react-router';
import { fetchReport, fetchReports, reportId, type BroadcastReport, type ReportHeader } from '../../shared/api/reports';
import { channelIdSchema } from '../../shared/api/fields';
import { usePolling } from '../../shared/api/usePolling';

const number = (value: number | null) => value === null ? '—' : value.toLocaleString('ko-KR');
const date = (value: string) => new Date(value).toLocaleString('ko-KR', { month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false });
const state = { LIVE: '방송 중 관측', OFFLINE: '관측 세션 종료', UNKNOWN: '상태 미확인' };
const reason = { UNCONFIRMED: '종료 미확정', OFFLINE_OBSERVED: '종료 상태 관측', SOURCE_CHANGED: '다음 방송 관측' };
export function ReportsPage() {
  const { channelId = '', reportId: id } = useParams();
  if (id ? !reportId.safeParse(id).success : !channelIdSchema.safeParse(channelId).success)
    return <section className="empty-state"><h1>방송 기록을 찾을 수 없어요</h1><Link to="/explore">방송 탐색으로 돌아가기</Link></section>;
  return id ? <ReportDetail key={id} id={id}/> : <ReportList key={channelId} channelId={channelId}/>;
}
function ReportList({ channelId }: { channelId: string }) {
  const result = usePolling(`reports:${channelId}`, signal => fetchReports(channelId, signal), 15000);
  return <section className="reports-page">
    <Link className="back-link" to={`/channels/${channelId}`}>← 채널 상세</Link>
    <div className="page-heading"><div><p className="eyebrow">관측한 방송 기록</p><h1>방송별 요약</h1></div></div>
    <p className="section-description">최근 7일에 관측한 방송을 살펴봐요. 종료가 확인되지 않은 기록은 이전 날짜여도 남겨둬요. 실제 방송 시작·종료 시각과는 다를 수 있어요.</p>
    {result.error && <p className="notice" role="alert">방송 기록을 불러오지 못했어요. {result.data && '이전 조회 결과예요.'} <button onClick={result.retry}>다시 시도</button></p>}
    {result.loading && !result.data && <p role="status">방송 기록을 확인하고 있어요.</p>}
    {result.data && <>
      {result.data.status === 'LAGGING' && <p className="notice">요약 갱신이 지연되거나 진행을 확인하지 못했어요.</p>}
      {result.data.unassignedCount > 0 && <p className="scope-note">방송을 구분하지 못한 채팅 {number(result.data.unassignedCount)}건은 요약에 넣지 않았어요.</p>}
      {result.data.truncated && <p className="notice">최근 관측 기록 100개를 표시해요.</p>}
      {result.data.reports.length ? <ul className="report-list">{result.data.reports.map(report => <li key={report.reportId}>
        <div><span className="report-state">{state[report.state]} · {reason[report.endReason]}</span><h2><Link to={`/reports/${report.reportId}`}>{date(report.observedFrom)} 관측</Link></h2><p>{report.identity === 'SOURCE_BROADCAST' ? '방송 ID 확인' : '내부 관측 세션'} · 마지막 관측 {date(report.observedTo)}</p></div>
        <div><span>관측 시청자 최고값</span><strong>{number(report.viewerMaximum)}<small> 명</small></strong></div>
      </li>)}</ul> : <div className="empty-state"><h2>아직 방송 관측 기록이 없어요</h2><p>수집하기 전의 방송 기록을 채워 넣지 않아요.</p></div>}
    </>}
  </section>;
}
function ReportDetail({ id }: { id: string }) {
  const result = usePolling(`report:${id}`, signal => fetchReport(id, signal), 15000);
  return <section className="reports-page">
    {result.data && <Link className="back-link" to={`/channels/${result.data.header.channelId}/reports`}>← 방송별 요약 목록</Link>}
    <div className="page-heading"><div><p className="eyebrow">관측 구간의 반응</p><h1>방송 요약</h1></div></div>
    {result.error && <p className="notice" role="alert">리포트를 불러오지 못했어요. {result.data ? '이전 조회 결과예요.' : '기록이 없거나 보존 기간이 지났을 수 있어요.'} <button onClick={result.retry}>다시 시도</button></p>}
    {result.loading && !result.data && <p role="status">방송 요약을 확인하고 있어요.</p>}
    {result.data && <ReportBody report={result.data}/>}
  </section>;
}
function ObservationRange({ header }: { header: ReportHeader }) {
  return <div className="report-range"><strong>{date(header.observedFrom)} – {date(header.observedTo)}</strong><p>{state[header.state]} · {reason[header.endReason]} · {header.identity === 'SOURCE_BROADCAST' ? '방송 ID 확인' : '내부 관측 세션'}</p>
    <p>일부 구간의 관측 결과예요. 방송의 실제 시작·종료나 전체 누적 수치를 뜻하지 않아요.</p>{header.identityUnconfirmed && <p>방송 구분을 확인하지 못한 표본이 포함돼 있어요.</p>}</div>;
}
function ReportBody({ report }: { report: BroadcastReport }) {
  const max = Math.max(1, ...report.points.map(p => p.count ?? 0));
  return <>
    <ObservationRange header={report.header}/>
    {report.status === 'LAGGING' && <p className="notice">요약 갱신이 지연돼요. 품질 건수와 피크는 진행 확인 후 표시해요.</p>}
    <dl className="report-metrics">
      <div><dt>관측 채팅 · 참고</dt><dd>{number(report.referenceCount)}<small> 건</small></dd></div>
      <div><dt>품질 충족 구간 채팅</dt><dd>{number(report.qualifiedCount)}<small> 건</small></dd></div>
      <div><dt>완료 1분 최대 채팅</dt><dd>{number(report.maximumPerMinute)}<small> 건</small></dd></div>
      <div><dt>관측 시청자 최고값</dt><dd>{number(report.header.viewerMaximum)}<small> 명</small></dd></div>
    </dl>
    <p className="scope-note">완료 {number(report.completedMinutes)}분 중 품질 충족 {number(report.healthyMinutes)}분 · 수집 관측 95% 이상이며 방송 구분을 분 전체에서 확인한 구간만 최대값과 피크에 포함해요.</p>
    {report.lateCount > 0 && <p className="notice">120초 이상 늦게 도착한 채팅 {number(report.lateCount)}건을 반영했어요. 과거 요약은 정정될 수 있어요.</p>}
    {report.unassignedChannelCount > 0 && <p className="scope-note">이 채널에서 방송을 구분하지 못한 채팅 {number(report.unassignedChannelCount)}건은 별도로 남아 있어요.</p>}
    <h2>반응 피크</h2><p>겹치는 구간을 합친 상위 3개예요. 건수가 같으면 먼저 관측한 시각 순으로 표시해요.</p>
    {report.peaks.length ? <ol className="report-peaks">{report.peaks.map(peak => <li key={peak.peakAt}><strong>{date(peak.from)} – {date(peak.to)}</strong><span>분당 최대 {number(peak.maximum)}건 · 피크 {date(peak.peakAt)}</span></li>)}</ol> : <p className="notice">품질을 확인한 양수 구간이 없어 피크를 표시하지 않아요.</p>}
    <section className="report-chart" aria-label="방송 리포트 분별 채팅"><h2>관측 구간별 채팅</h2>
      {report.timelineTruncated && <p>그래프와 표는 마지막 6시간이에요. 위 요약은 전체 관측 구간 기준이에요.</p>}
      {report.points.length > 0 ? <><svg viewBox={`0 0 ${report.points.length * 10} 150`} preserveAspectRatio="none" role="img" aria-label="분당 채팅 막대그래프. 음영은 미확인 구간이며 수치는 아래 표에서 확인할 수 있어요.">
        {report.points.map((point, i) => <rect key={point.from} x={i * 10 + 1} width="8" y={point.count === null ? 0 : 140 - point.count / max * 130} height={point.count === null ? 140 : point.count / max * 130} className={point.count === null ? 'category-gap' : 'category-bar'}><title>{date(point.from)} {point.count === null ? '미확인' : `${number(point.count)}건`}</title></rect>)}
      </svg><p>음영은 공백·품질 미확인 구간이에요. 정상 관측된 0건과 구분해요.</p>
      <details><summary>분별 건수와 품질 보기</summary><div className="category-table"><table><thead><tr><th>시각</th><th>채팅</th><th>참고</th><th>수집 관측률</th></tr></thead><tbody>{report.points.map(p => <tr key={p.from}><td>{date(p.from)}</td><td>{number(p.count)}</td><td>{number(p.referenceCount)}</td><td>{Math.floor(p.collectionCoverage * 100)}%</td></tr>)}</tbody></table></div></details></> : <p>완료된 1분 구간이 아직 없어요.</p>}
    </section>
    <p className="report-version">리포트 버전 <span title={report.header.reportVersion}>{report.header.reportVersion.slice(-12)}</span> · 집계 반영 {date(report.header.updatedAt)} · 조회 {date(report.asOf)}</p>
    <p className="scope-note">기록은 관측과 보존된 입력 범위에 한정돼요. 원본 보존 기간이 지나면 리포트 전체를 다시 만들 수 없어요.</p>
  </>;
}
