// 공통 내비게이션과 화면 정의. 신규 화면은 이 목록에 등록하고 본문 렌더러를 연결한다.
const platformIcons = {
  home: '<path d="m3 10 9-7 9 7v10a1 1 0 0 1-1 1h-5v-7H9v7H4a1 1 0 0 1-1-1Z"/>',
  explore: '<circle cx="12" cy="12" r="9"/><path d="m16 8-3 5-5 3 3-5Z"/>',
  surges: '<path d="m3 17 6-6 4 4 8-10M14 5h7v7"/>',
  saved: '<path d="M6 4h12v17l-6-4-6 4Z"/>',
  search: '<circle cx="10" cy="10" r="6"/><path d="m15 15 5 5"/>'
};
const platformViews = {
  home: { label: '홈', title: '오늘의 발견', description: '채팅이 모이는 순간부터, 새로운 방송을 만나보세요.', sectionTitle: '지금 활발한 방송', sectionDescription: '수집한 채널의 최근 채팅량 순서예요.', hero: true, rail: true },
  explore: { label: '방송 탐색', title: '방송 탐색', description: '오늘 끌리는 카테고리에서 다음 방송을 찾아보세요.', sectionTitle: '방송 둘러보기', sectionDescription: '수집한 채널의 최근 채팅량 순서예요.' },
  surges: { label: '반응 급상승', title: '지금, 반응이 모이는 순간', description: '평소보다 채팅이 빠르게 늘어난 방송을 만나보세요.', sectionTitle: '최근 반응 급상승', sectionDescription: '직전 5분 평균 대비 증가 배수가 높은 순서예요.', surgeOnly: true },
  saved: { label: '관심 방송', title: '다시 만나고 싶은 방송', description: '눈여겨본 방송을 한곳에 모아두세요.', sectionTitle: '내가 저장한 방송', sectionDescription: '이 브라우저에 저장한 관심 방송이에요.', savedOnly: true },
  search: { label: '검색 결과', title: '방송 검색', description: '방송 제목, 채널 이름, 카테고리로 찾아보세요.', sectionTitle: '검색한 방송', sectionDescription: '현재 수집 대상 안에서 검색합니다.', hidden: true }
};
function platformIcon(name) { return `<svg viewBox="0 0 24 24" aria-hidden="true">${platformIcons[name] || platformIcons.explore}</svg>`; }
function navigationMarkup() {
  return Object.entries(platformViews).filter(([,view]) => !view.hidden).map(([id, view]) => `<a class="platform-nav-link" href="#/${id}" data-route="${id}">${platformIcon(id)}<span>${view.label}</span>${id === 'saved' ? '<span class="nav-saved-count" data-saved-count hidden>0</span>' : ''}</a>`).join('');
}
function readPlatformRoute() {
  const raw = location.hash.slice(1);
  const aliases = { ranking: 'explore', surges: 'surges', top: 'home' };
  const [path, query = ''] = raw.split('?');
  const candidate = aliases[path] || path.replace(/^\//, '') || 'home';
  return { view: Object.hasOwn(platformViews, candidate) ? candidate : 'home', params: new URLSearchParams(query) };
}
function renderPlatformNavigation() {
  document.querySelectorAll('[data-navigation]').forEach(element => { element.innerHTML = navigationMarkup(); });
}
function updatePlatformShell(viewId, savedCount) {
  const view = platformViews[viewId];
  document.body.dataset.view = viewId;
  document.title = `${view.label} · Chat Pulse`;
  document.querySelector('#page-title').textContent = view.title;
  document.querySelector('#page-description').textContent = view.description;
  document.querySelector('.discovery-hero').hidden = !view.hero;
  document.querySelector('#surges').hidden = !view.rail;
  document.querySelectorAll('[data-route]').forEach(link => {
    if (link.dataset.route === viewId) link.setAttribute('aria-current', 'page'); else link.removeAttribute('aria-current');
  });
  document.querySelectorAll('[data-saved-count]').forEach(badge => { badge.hidden = savedCount === 0; badge.textContent = savedCount; });
  document.querySelector('#home-explore-link').hidden = viewId !== 'home';
  document.querySelector('#ranking .segments').hidden = Boolean(view.surgeOnly);
}
