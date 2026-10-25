import { Link } from 'react-router';
import { channelName, formatCount, type ChannelSummary } from './model';
import { Icon } from '../../shared/ui/Icon';
export function BroadcastCard({ channel, seconds, saved, onSave }: {
  channel: ChannelSummary; seconds: number; saved: boolean; onSave: (id: string) => void;
}) {
  return <li className="broadcast-card">
    <Link className="card-visual" to={`/channels/${channel.id}`} aria-label={`${channelName(channel.id)} 상세 보기`}>
      <span className="thumbnail-placeholder"><Icon name="broadcast"/><span>방송 이미지 준비 중</span></span>
      <span className="image-caption">채널 {channel.id.slice(0, 8)}</span>
    </Link>
    <button className="save-button" onClick={() => onSave(channel.id)} aria-label={`${channelName(channel.id)} 관심 방송 ${saved ? '해제' : '저장'}`} aria-pressed={saved}><Icon name="bookmark"/></button>
    <div className="card-copy"><span className="avatar" aria-hidden="true">{channel.id.slice(0, 2).toUpperCase()}</span><div>
      <h3><Link to={`/channels/${channel.id}`}>{channelName(channel.id)}</Link></h3><p className="card-byline">방송 정보 확인 전</p>
      <div className="card-bottom"><span>{channel.chatCount === null ? '최근 집계 없음' : `${seconds / 60}분 수집 채팅`}</span><strong>{channel.chatCount === null ? '—' : formatCount(channel.chatCount)}</strong></div>
    </div></div>
  </li>;
}
