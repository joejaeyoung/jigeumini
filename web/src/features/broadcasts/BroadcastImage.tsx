import { useState } from 'react';
import type { BroadcastMetadata } from '../../shared/api/client';
import { Icon } from '../../shared/ui/Icon';
import { metadataLabel } from './model';
export function BroadcastImage({ metadata }: { metadata?: BroadcastMetadata }) {
  const [failed, setFailed] = useState<string | null>(null);
  const url = metadata?.liveStatus === 'LIVE' && metadata.freshness === 'FRESH' ? metadata.thumbnailUrl : null;
  return <>
    {url && url !== failed
      ? <img className="broadcast-image" src={url} alt={metadata?.title ? `${metadata.title} 방송 미리보기` : '방송 미리보기'} loading="lazy" referrerPolicy="no-referrer" onError={() => setFailed(url)}/>
      : <span className="thumbnail-placeholder"><Icon name="broadcast"/><span>{metadata?.liveStatus === 'RESTRICTED' ? '접근이 제한된 방송' : metadata?.liveStatus === 'OFFLINE' ? '다음 방송에서 만나요' : '방송 이미지 준비 중'}</span></span>}
    <span className="broadcast-status" data-live={metadata?.liveStatus === 'LIVE' && metadata.freshness === 'FRESH'}>{metadataLabel(metadata)}</span>
  </>;
}
