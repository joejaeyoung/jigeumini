import { useEffect, useRef, useState, type FormEvent } from 'react';
import { Link, NavLink, Route, Routes, useLocation, useNavigate } from 'react-router';
import { BrowsePage } from '../features/broadcasts/BrowsePage';
import { ChannelPage } from '../features/broadcasts/ChannelPage';
import { useSavedChannels } from '../features/saved/useSavedChannels';
import { Icon } from '../shared/ui/Icon';
import { Modal } from '../shared/ui/Modal';
const navigation = [{ to: '/', label: '홈' }, { to: '/explore', label: '방송 탐색' }, { to: '/surges', label: '반응 급상승' }, { to: '/saved', label: '관심 방송' }];
export function App() {
  const { saved, toggle, storageError } = useSavedChannels();
  const [info, setInfo] = useState(false);
  const [search, setSearch] = useState('');
  const searchRef = useRef<HTMLInputElement>(null);
  const mainRef = useRef<HTMLElement>(null);
  const firstRoute = useRef(true);
  const location = useLocation();
  const navigate = useNavigate();
  useEffect(() => {
    setSearch(new URLSearchParams(location.search).get('q') ?? '');
    const title = navigation.find(item => item.to === location.pathname)?.label ?? (location.pathname === '/search' ? '방송 검색' : '방송 상세');
    document.title = `${title} · 지금이니`;
    if (firstRoute.current) firstRoute.current = false;
    else if (!new URLSearchParams(location.search).has('window')) { window.scrollTo(0, 0); mainRef.current?.focus({ preventScroll: true }); }
  }, [location.pathname, location.search]);
  useEffect(() => {
    const keydown = (event: KeyboardEvent) => {
      const target = event.target;
      if (event.key === '/' && !event.metaKey && !event.ctrlKey && !event.altKey && !document.querySelector('dialog[open]') && !(target instanceof Element && target.closest('input,textarea,select,[contenteditable="true"]'))) {
        event.preventDefault(); searchRef.current?.focus();
      }
    };
    document.addEventListener('keydown', keydown);
    return () => document.removeEventListener('keydown', keydown);
  }, []);
  function submitSearch(event: FormEvent) { event.preventDefault(); navigate(`/search?${new URLSearchParams({ q: search.trim() })}`); }
  return <>
    <a className="skip" href="#main">본문으로 바로가기</a>
    <header className="site-header"><div className="header-inner"><Link className="wordmark" to="/" aria-label="지금이니 홈">지금이니</Link><nav aria-label="주요 메뉴">{navigation.map(item => <NavLink key={item.to} to={item.to} end>{item.label}{item.to === '/saved' && saved.length > 0 && <span className="saved-count">{saved.length}</span>}</NavLink>)}</nav><form className="search-box" role="search" onSubmit={submitSearch}><button type="submit" aria-label="검색"><Icon name="search"/></button><label className="sr-only" htmlFor="site-search">채널 ID 검색</label><input ref={searchRef} id="site-search" type="search" value={search} onChange={event => setSearch(event.target.value)} placeholder="채널 ID 검색" autoComplete="off"/><kbd aria-hidden="true">/</kbd></form><button className="info-button" aria-label="서비스 안내" onClick={() => setInfo(true)}>?</button></div></header>
    <main id="main" ref={mainRef} tabIndex={-1} className="container">
      {storageError && <p className="notice" role="alert">브라우저 저장 공간을 사용할 수 없어 관심 방송은 현재 화면에서만 유지돼요.</p>}
      <Routes>
        <Route path="/" element={<BrowsePage view="home" saved={saved} toggle={toggle}/>}/>
        <Route path="/explore" element={<BrowsePage view="explore" saved={saved} toggle={toggle}/>}/>
        <Route path="/saved" element={<BrowsePage view="saved" saved={saved} toggle={toggle}/>}/>
        <Route path="/search" element={<BrowsePage view="search" saved={saved} toggle={toggle}/>}/>
        <Route path="/surges" element={<><div className="page-heading"><div><h1>지금, 반응이 모이는 순간</h1><p>평소보다 채팅 반응이 늘어난 방송을 모아볼 공간이에요.</p></div></div><section className="empty-state"><Icon name="trend"/><h2>급상승 분석을 준비하고 있어요</h2><p>현재는 수집된 채팅 건수만 볼 수 있어요.<br/>반응 배수와 급상승 방송은 분석이 준비되면 제공할게요.</p><Link className="primary-button" to="/explore">수집된 방송 둘러보기 <Icon name="arrow"/></Link></section></>}/>
        <Route path="/channels/:channelId" element={<ChannelPage saved={saved} toggle={toggle}/>}/>
        <Route path="*" element={<section className="empty-state"><h1>페이지를 찾을 수 없어요</h1><p>주소를 확인하거나 홈으로 돌아가주세요.</p><Link className="primary-button" to="/">홈으로 돌아가기</Link></section>}/>
      </Routes>
      <footer><Link className="wordmark" to="/">지금이니</Link><button onClick={() => setInfo(true)}>서비스와 집계 기준</button><span>채팅으로 발견하는 새로운 방송</span></footer>
    </main>
    {info && <Modal title="어떤 반응을 보여주나요?" onClose={() => setInfo(false)}><div className="modal-body"><h3>수집한 채널 안에서 살펴봐요</h3><p>최근 1분·5분 동안 수집된 일반 채팅 건수를 보여줘요. 전체 치지직 순위나 시청자 수를 뜻하지 않아요.</p><h3>기록이 없다고 0건은 아니에요</h3><p>진행 중인 10초 구간이 포함되며, 수집이 끊긴 구간은 아직 측정하지 않아요. 채팅이 수집되지 않은 시간대를 0건으로 채우지 않아요.</p><h3>방송 정보와 급상승은 준비 중이에요</h3><p>방송 제목·썸네일·방송 여부와 검증된 반응 배수는 아직 제공하지 않아요. 홈 이미지는 방송 화면이 아닌 서비스 일러스트예요.</p><h3>관심 방송은 이 브라우저에 저장해요</h3><p>로그인 없이 현재 브라우저에 저장돼요. 브라우저 데이터를 지우면 함께 삭제돼요.</p><button className="primary-button" onClick={() => setInfo(false)}>확인했어요</button></div></Modal>}
  </>;
}
