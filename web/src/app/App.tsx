import { useEffect, useRef, useState, type FormEvent } from 'react';
import { Link, NavLink, Route, Routes, useLocation, useNavigate } from 'react-router';
import { BrowsePage } from '../features/broadcasts/BrowsePage';
import { NotificationsPage } from '../features/notifications/NotificationsPage';
import { CategoriesPage } from '../features/categories/CategoriesPage';
import { SurgesPage } from '../features/broadcasts/SurgesPage';
import { ChannelPage } from '../features/broadcasts/ChannelPage';
import { useSavedChannels } from '../features/saved/useSavedChannels';
import { Icon } from '../shared/ui/Icon';
import { Modal } from '../shared/ui/Modal';
const navigation = [{ to: '/', label: '홈' }, { to: '/explore', label: '방송 탐색' }, { to: '/surges', label: '반응 급상승' }, { to: '/categories', label: '카테고리' }, { to: '/saved', label: '관심 방송' }, { to: '/notifications', label: '알림' }];
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
    const title = navigation.find(item => item.to === location.pathname)?.label ?? (location.pathname === '/search' ? '방송 검색' : location.pathname.startsWith('/categories/') ? '카테고리 상세' : '방송 상세');
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
    <header className="site-header"><div className="header-inner"><Link className="wordmark" to="/" aria-label="지금이니 홈">지금이니</Link><nav aria-label="주요 메뉴">{navigation.map(item => <NavLink key={item.to} to={item.to} end>{item.label}{item.to === '/saved' && saved.length > 0 && <span className="saved-count">{saved.length}</span>}</NavLink>)}</nav><form className="search-box" role="search" onSubmit={submitSearch}><button type="submit" aria-label="검색"><Icon name="search"/></button><label className="sr-only" htmlFor="site-search">방송 제목·채널명 검색</label><input ref={searchRef} id="site-search" type="search" value={search} onChange={event => setSearch(event.target.value)} placeholder="방송 제목·채널명 검색" autoComplete="off"/><kbd aria-hidden="true">/</kbd></form><button className="info-button" aria-label="서비스 안내" onClick={() => setInfo(true)}>?</button></div></header>
    <main id="main" ref={mainRef} tabIndex={-1} className="container">
      {storageError && <p className="notice" role="alert">브라우저 저장 공간을 사용할 수 없어 관심 방송은 현재 화면에서만 유지돼요.</p>}
      <Routes>
        <Route path="/" element={<BrowsePage view="home" saved={saved} toggle={toggle}/>}/>
        <Route path="/explore" element={<BrowsePage view="explore" saved={saved} toggle={toggle}/>}/>
        <Route path="/saved" element={<BrowsePage view="saved" saved={saved} toggle={toggle}/>}/>
        <Route path="/search" element={<BrowsePage view="search" saved={saved} toggle={toggle}/>}/>
        <Route path="/notifications" element={<NotificationsPage saved={saved}/>}/>
        <Route path="/categories" element={<CategoriesPage/>}/>
        <Route path="/categories/:categoryKey" element={<CategoriesPage/>}/>
        <Route path="/surges" element={<SurgesPage saved={saved} toggle={toggle}/>}/>
        <Route path="/channels/:channelId" element={<ChannelPage saved={saved} toggle={toggle}/>}/>
        <Route path="*" element={<section className="empty-state"><h1>페이지를 찾을 수 없어요</h1><p>주소를 확인하거나 홈으로 돌아가주세요.</p><Link className="primary-button" to="/">홈으로 돌아가기</Link></section>}/>
      </Routes>
      <footer><Link className="wordmark" to="/">지금이니</Link><button onClick={() => setInfo(true)}>서비스와 집계 기준</button><span>채팅으로 발견하는 새로운 방송</span></footer>
    </main>
    {info && <Modal title="어떤 반응을 보여주나요?" onClose={() => setInfo(false)}><div className="modal-body"><h3>수집한 채널 안에서 살펴봐요</h3><p>완료된 1분·5분의 일반 채팅 건수를 보여줘요. 연결 관측 95% 이상이고 수집·발행·분석 상태를 확인한 방송만 순위에 포함해요. 전체 치지직 순위나 시청자 수를 뜻하지 않아요.</p><h3>기록이 없다고 0건은 아니에요</h3><p>진행 중인 구간은 제외해요. 공백·손실·지연이 확인되면 순위에서 제외하고, 관측이 정상인 조용한 방송만 0건으로 표시해요. 연결 관측률은 원천 채팅의 완전한 수신율을 보장하지 않아요.</p><h3>방송 정보와 채팅 집계는 별도로 확인해요</h3><p>공개 방송 제목·썸네일·방송 상태를 주기적으로 확인해요. 오래되거나 제한된 방송 정보는 구분해 표시하며, 이미지가 없으면 대체 화면을 보여줘요. 급상승은 최근 1분 100건 이상, 평소보다 50건 이상 증가, 2배 이상을 두 번 연속 확인한 결과예요. 같은 방송은 5분 뒤부터 다시 판단해요. 홈 대표 이미지는 서비스 일러스트예요.</p><h3>관심 방송은 이 브라우저에 저장해요</h3><p>로그인 없이 현재 브라우저에 저장돼요. 브라우저 데이터를 지우면 함께 삭제돼요.</p><button className="primary-button" onClick={() => setInfo(false)}>확인했어요</button></div></Modal>}
  </>;
}
