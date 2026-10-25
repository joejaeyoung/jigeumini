import { Link } from 'react-router';
import { channelName, formatCount, type ChannelSummary } from './model';
import { Icon } from '../../shared/ui/Icon';
import { qualityLabel } from '../../shared/api/quality';
import { ReactionRibbon } from '../../shared/ui/ReactionRibbon';
import { BroadcastImage } from './BroadcastImage';
export function BroadcastCard({ channel, seconds, saved, onSave, reaction, recorded = false, countsKnown = true }: {
  countsKnown?: boolean; channel: ChannelSummary; seconds: number; saved: boolean; onSave: (id: string) => void; reaction?: number; recorded?: boolean;
}) {
  const meta = channel.metadata;
  const name = meta?.channelName ?? channelName(channel.id);
  return <li className="broadcast-card" data-reaction={Boolean(reaction)}>
    <Link className="card-visual" to={`/channels/${channel.id}`} aria-label={`${name} 상세 보기`}>
      <BroadcastImage metadata={meta}/>
      {reaction !== undefined && <ReactionRibbon multiplier={reaction}/>}
    </Link>
    <button className="save-button" onClick={() => onSave(channel.id)} aria-label={`${name} 관심 방송 ${saved ? '해제' : '저장'}`} aria-pressed={saved}><Icon name="bookmark"/></button>
    <div className="card-copy"><span className="avatar" aria-hidden="true">{meta?.channelName?.slice(0, 1) ?? channel.id.slice(0, 2).toUpperCase()}</span><div>
      <h3><Link to={`/channels/${channel.id}`} title={meta?.title ?? name}>{meta?.title ?? name}</Link></h3><p className="card-byline">{name}{meta?.categoryName && <span> · {meta.categoryName}</span>}</p>
      <div className="card-bottom"><span>{!countsKnown ? '집계 확인 전' : recorded ? '기록 당시 1분 채팅' : channel.stale ? '순위 갱신 대기' : channel.quality && channel.quality.status !== 'HEALTHY' ? qualityLabel[channel.quality.status] : channel.chatCount === null ? '최근 집계 없음' : `완료된 ${seconds / 60}분 채팅`}</span><strong>{channel.chatCount === null ? '—' : formatCount(channel.chatCount)}</strong></div>
      {channel.quality && <p className="quality-caption">{channel.quality.status === 'HEALTHY' && !channel.stale ? `${channel.quality.rank}위 · 관측 ${(channel.quality.coverage * 100).toFixed(1)}%${channel.quality.previousRank === null ? '' : ` · 이전 ${channel.quality.previousRank}위`}` : `순위 제외 · 참고 수집 ${formatCount(channel.quality.referenceCount)}건`}</p>}
    </div></div>
  </li>;
}
