import { Link } from 'react-router';
import { channelName, formatCount, type ChannelSummary } from './model';
import { Icon } from '../../shared/ui/Icon';
import { BroadcastImage } from './BroadcastImage';
export function BroadcastCard({ channel, seconds, saved, onSave }: {
  channel: ChannelSummary; seconds: number; saved: boolean; onSave: (id: string) => void;
}) {
  const meta = channel.metadata;
  const name = meta?.channelName ?? channelName(channel.id);
  return <li className="broadcast-card">
    <Link className="card-visual" to={`/channels/${channel.id}`} aria-label={`${name} 상세 보기`}>
      <BroadcastImage metadata={meta}/>
    </Link>
    <button className="save-button" onClick={() => onSave(channel.id)} aria-label={`${name} 관심 방송 ${saved ? '해제' : '저장'}`} aria-pressed={saved}><Icon name="bookmark"/></button>
    <div className="card-copy"><span className="avatar" aria-hidden="true">{meta?.channelName?.slice(0, 1) ?? channel.id.slice(0, 2).toUpperCase()}</span><div>
      <h3><Link to={`/channels/${channel.id}`} title={meta?.title ?? name}>{meta?.title ?? name}</Link></h3><p className="card-byline">{name}{meta?.categoryName && <span> · {meta.categoryName}</span>}</p>
      <div className="card-bottom"><span>{channel.chatCount === null ? '최근 집계 없음' : `${seconds / 60}분 수집 채팅`}</span><strong>{channel.chatCount === null ? '—' : formatCount(channel.chatCount)}</strong></div>
    </div></div>
  </li>;
}
