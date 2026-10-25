// 서버가 검증한 배수를 제공할 때 사용한다. 현재 M1 응답에서는 렌더하지 않는다.
export function ReactionRibbon({ multiplier }: { multiplier: number }) {
  if (!Number.isFinite(multiplier) || multiplier < 2) return null;
  return <span className="reaction-ribbon" data-level={multiplier >= 3 ? 'high' : 'rising'}><span aria-hidden="true">↗</span> 반응 <strong>{multiplier.toFixed(1)}<small>배</small></strong></span>;
}
