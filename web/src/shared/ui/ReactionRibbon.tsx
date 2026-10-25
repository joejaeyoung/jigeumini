// 품질과 연속 조건이 확인된 급상승 이벤트에만 사용한다.
export function ReactionRibbon({ multiplier }: { multiplier: number }) {
  if (!Number.isFinite(multiplier) || multiplier < 2) return null;
  return <span className="reaction-ribbon" data-level={multiplier >= 3 ? 'high' : 'rising'}><span aria-hidden="true">↗</span> 반응 <strong>{(Math.floor(multiplier * 10) / 10).toFixed(1)}<small>배</small></strong></span>;
}
