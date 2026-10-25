// 화면 설계용 합성 데이터. 외부 서비스나 실제 채널 정보는 사용하지 않는다.
const channels = [
  { id: 'sand', name: '모래별', initial: '모', category: '게임', title: '저 문 너머엔 뭐가 있을까?', scene: 'adventure', five: 8420, one: 2000, surge: true, base: 625, multiplier: '3.2', eventTime: '21:38', seed: 2 },
  { id: 'walk', name: '오후산책', initial: '오', category: '토크', title: '우리 잠깐, 주말 이야기할까요?', scene: 'talk', five: 6230, one: 920, surge: true, base: 400, multiplier: '2.1', eventTime: '21:33', seed: 5 },
  { id: 'moon', name: '달빛항해', initial: '달', category: '게임', title: '이번 코너만 넘으면 1등인데', scene: 'racing', five: 4780, one: 1040, surge: false, seed: 8 },
  { id: 'blue', name: '파란오후', initial: '파', category: '음악', title: '해 질 무렵, 오늘의 신청곡', scene: 'music', five: 3150, one: 720, surge: true, base: 250, multiplier: '2.6', eventTime: '21:36', seed: 11 },
  { id: 'cloud', name: '느린구름', initial: '느', category: '토크', title: '커피 한 잔만큼 느린 일요일', scene: 'talk', five: 2460, one: 480, surge: false, seed: 14 },
  { id: 'forest', name: '초록숲', initial: '초', category: '게임', title: '길 잃어도 괜찮은 초록빛 모험', scene: 'adventure', five: 1820, one: 340, surge: false, seed: 17 }
];
const state = { window: 5, category: '전체', selected: 'sand', range: 30, query: '', savedOnly: false, view: 'home' };
let saved = new Set();
try { const stored = JSON.parse(localStorage.getItem('chatpulse-design-saved') || '[]'); if (Array.isArray(stored)) saved = new Set(stored.filter(id => channels.some(c => c.id === id))); } catch { /* 저장소가 없어도 현재 화면에서 관심 방송을 선택할 수 있다. */ }
const bookmark = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 4h12v17l-6-4-6 4Z"/></svg>';
const format = value => value.toLocaleString('ko-KR');
const surgeLevel = multiplier => Number(multiplier) >= 3 ? 'high' : 'rising';
const count = channel => state.window === 5 ? channel.five : channel.one;
const announce = message => { document.querySelector('#announcement').textContent = message; };

function valuesFor(channel, size) {
  return Array.from({length: size}, (_, i) => {
    const baseline = channel.one * .27;
    const variation = (Math.sin(i * 1.4 + channel.seed) + 1) * channel.one * .12;
    const rise = channel.surge && i > size - 8 ? ((i - size + 8) / 7) * channel.one * .65 : 0;
    return Math.round(baseline + variation + rise);
  });
}
function sparkline(channel) {
  const data = valuesFor(channel, 20);
  const max = Math.max(...data) * 1.1;
  const points = data.map((value, i) => `${(i / 19 * 85 + 1).toFixed(1)},${(30 - value / max * 27).toFixed(1)}`).join(' ');
  return `<svg class="sparkline" viewBox="0 0 88 32" aria-hidden="true"><polyline points="${points}"/></svg>`;
}
function renderList() {
  const view = platformViews[state.view];
  const sorted = [...channels].sort((a,b) => view.surgeOnly ? Number(b.multiplier || 0) - Number(a.multiplier || 0) : count(b) - count(a) || a.id.localeCompare(b.id));
  const visible = sorted.filter(c => (!view.surgeOnly || c.surge) && (state.category === '전체' || c.category === state.category) && (!state.savedOnly || saved.has(c.id)) && `${c.name} ${c.title} ${c.category}`.includes(state.query));
  document.querySelector('#channel-list').innerHTML = visible.map(c => `<li class="broadcast-card"><button class="card-visual thumbnail scene-${c.scene}" data-channel="${c.id}" aria-label="${c.name}, ${c.title}, 방송 자세히 보기"><span class="card-rank">${sorted.indexOf(c)+1}</span>${c.surge ? `<span class="card-surge" data-surge-level="${surgeLevel(c.multiplier)}"><span class="surge-arrow" aria-hidden="true">↗</span><span>반응</span><strong>${c.multiplier}<span class="surge-unit">배</span></strong></span>` : ''}</button><button class="save-button" data-save="${c.id}" aria-pressed="${saved.has(c.id)}" aria-label="${c.name} 관심 방송 ${saved.has(c.id) ? '해제' : '저장'}">${bookmark}</button><div class="card-copy"><span class="avatar avatar-${c.id}" aria-hidden="true">${c.initial}</span><div><h3><button class="card-title" data-channel="${c.id}">${c.title}</button></h3><div class="card-byline"><span>${c.name}</span><span>·</span><span>예시 방송</span></div><div class="card-bottom"><span class="category-tag">${c.category}</span><span class="card-chat">${state.window}분 채팅 <b>${format(count(c))}</b></span></div></div></div></li>`).join('');
  document.querySelectorAll('[data-window-label]').forEach(el => { el.textContent = `${state.window}분`; });
  document.querySelector('#result-count').textContent = `${visible.length}개 방송`;
  document.querySelector('#empty-state').hidden = visible.length > 0;
  document.querySelector('#ranking-heading').textContent = view.sectionTitle;
  document.querySelector('#results-description').textContent = state.view === 'search' && state.query ? `“${state.query}” 검색 결과예요.` : view.sectionDescription;
  const emptyTitle = state.savedOnly && saved.size === 0 ? '다시 보고 싶은 방송을 저장해보세요' : '조건에 맞는 방송이 없어요';
  document.querySelector('#empty-state h3').textContent = emptyTitle;
  document.querySelector('#empty-state p').textContent = state.savedOnly && saved.size === 0 ? '방송 카드의 북마크를 누르면 이곳에서 다시 찾을 수 있어요.' : '다른 검색어나 카테고리로 다시 둘러보세요.';
  updatePlatformShell(state.view, saved.size);

}
function renderSurges() {
  document.querySelector('#surge-list').innerHTML = ['sand','blue','walk'].map(id => {
    const c = channels.find(item => item.id === id);
    return `<button class="surge-channel" data-select="${c.id}" aria-label="${c.name}, 반응 ${c.multiplier}배, 자세히 보기"><span class="avatar avatar-${c.id}" aria-hidden="true">${c.initial}</span><span><strong>${c.name}</strong><small>${c.category} · ${c.eventTime}</small></span><span class="multiplier" data-surge-level="${surgeLevel(c.multiplier)}"><span class="surge-arrow" aria-hidden="true">↗</span><strong>${c.multiplier}<span class="surge-unit">배</span></strong></span></button>`;
  }).join('');
}
function renderChart() {
  const channel = channels.find(c => c.id === state.selected);
  document.querySelector('#detail-heading').innerHTML = `${channel.name} <span class="detail-category">${channel.category}</span>`;
  document.querySelector('#detail-count').textContent = format(count(channel));
  document.querySelector('#detail-caption').textContent = channel.surge ? `${channel.eventTime} 반응 급증 · 직전 5분 평균 대비 ${channel.multiplier}배` : '표시 구간에 급증 기록이 없습니다.';
  const width = Math.max(240, Math.round(document.querySelector('#chart').getBoundingClientRect().width) - 48);
  const height = 190, left = 34, right = width - 10, top = 12, bottom = 158;
  const data = valuesFor(channel, state.range);
  const max = Math.ceil(Math.max(...data) / 500) * 500;
  const coords = data.map((value, i) => [left + i / (data.length - 1) * (right - left), bottom - value / max * (bottom-top)]);
  const path = coords.map(([x,y],i) => `${i ? 'L' : 'M'}${x.toFixed(1)} ${y.toFixed(1)}`).join(' ');
  let grid = [0, .5, 1].map(r => { const y = bottom - r * (bottom-top); return `<line class="grid-line" x1="${left}" x2="${right}" y1="${y}" y2="${y}"/><text x="${left-8}" y="${y+4}" text-anchor="end">${format(max*r)}</text>`; }).join('');
  const ticks = width < 400 ? 2 : 4;
  for (let i=0; i<=ticks; i++) {
    const mins = 21 * 60 + 40 - state.range + state.range * i / ticks;
    const label = `${String(Math.floor(mins/60)).padStart(2,'0')}:${String(Math.floor(mins%60)).padStart(2,'0')}`;
    grid += `<text x="${left + i / ticks * (right-left)}" y="183" text-anchor="${i === 0 ? 'start' : i === ticks ? 'end' : 'middle'}">${label}</text>`;
  }
  const event = coords[coords.length-3];
  const marker = channel.surge ? `<line class="event-line" x1="${event[0]}" x2="${event[0]}" y1="${top}" y2="${bottom}"/><circle class="event-dot" cx="${event[0]}" cy="${event[1]}" r="5"/>` : '';
  document.querySelector('#chart').innerHTML = `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="${channel.name} 최근 ${state.range}분의 분당 채팅 수 추이. 합성 예시 데이터.">${grid}<path class="area" d="${path} L${right} ${bottom} L${left} ${bottom} Z"/><path class="line" d="${path}"/>${marker}</svg>`;
}
function selectChannel(id) {
  state.selected = id;
  const channel = channels.find(c => c.id === id);
  document.querySelector('#broadcast-title').textContent = channel.title;
  document.querySelector('#detail-image').className = `thumbnail scene-${channel.scene}`;
  document.querySelector('#watch-note').hidden = true;
  document.querySelector('#channel-dialog').showModal();
  document.querySelector('#channel-dialog').scrollTop = 0;
  renderChart();
  announce(`${channel.name} 방송 상세를 표시합니다.`);
}
function applyPlatformRoute({scroll = true, focus = false} = {}) {
  const route = readPlatformRoute();
  state.view = route.view;
  state.savedOnly = Boolean(platformViews[route.view].savedOnly);
  state.query = route.params.get('q') || '';
  state.category = ['전체','게임','토크','음악'].includes(route.params.get('category')) ? route.params.get('category') : '전체';
  document.querySelector('#channel-search').value = state.query;
  document.querySelectorAll('[data-category]').forEach(button => button.setAttribute('aria-pressed', String(button.dataset.category === state.category)));
  renderList();
  if (scroll) window.scrollTo({top:0, behavior:'instant'});
  if (focus) document.querySelector('#page-title').focus({preventScroll:true});
}
function navigatePlatform(view, params = {}, {replace = false, scroll = true, focus = false} = {}) {
  const query = new URLSearchParams(Object.entries(params).filter(([,value]) => value && value !== '전체')).toString();
  const hash = `#/${view}${query ? `?${query}` : ''}`;
  if (replace) history.replaceState(null,'',hash); else if (location.hash !== hash) history.pushState(null,'',hash);
  applyPlatformRoute({scroll,focus});
}
function resetFilters() { navigatePlatform('explore', {}, {focus:true}); }
window.addEventListener('hashchange', () => applyPlatformRoute({focus:true}));
window.addEventListener('popstate', () => applyPlatformRoute());
document.addEventListener('click', event => {
  if (event.target.closest('.skip')) { event.preventDefault(); document.querySelector('#top').focus({preventScroll:true}); return; }
  const routeLink = event.target.closest('a[href^="#/"]');
  if (routeLink && routeLink.hash === location.hash) { event.preventDefault(); applyPlatformRoute({focus:true}); }
  const saveButton = event.target.closest('[data-save]');
  if (saveButton) {
    const id = saveButton.dataset.save;
    if (saved.has(id)) saved.delete(id); else saved.add(id);
    try { localStorage.setItem('chatpulse-design-saved', JSON.stringify([...saved])); } catch { /* 현재 세션의 선택은 유지한다. */ }
    renderList();
    (document.querySelector(`[data-save="${id}"]`) || document.querySelector('#page-title')).focus({preventScroll:true});
    announce(`${channels.find(c => c.id === id).name} 관심 방송 ${saved.has(id) ? '저장' : '해제'}`);
  }
  const windowButton = event.target.closest('[data-window]');
  if (windowButton) {
    state.window = Number(windowButton.dataset.window);
    document.querySelectorAll('[data-window]').forEach(button => button.setAttribute('aria-pressed', String(button === windowButton)));
    renderList(); renderChart(); announce(`최근 ${state.window}분 순서를 표시합니다.`);
  }
  const categoryButton = event.target.closest('[data-category]');
  if (categoryButton) {
    navigatePlatform(state.view === 'home' ? 'explore' : state.view, {category:categoryButton.dataset.category,q:state.query}, {replace:true,scroll:false});
    announce(`${state.category} 카테고리 방송을 표시합니다.`);
  }
  const channelButton = event.target.closest('[data-channel], [data-select]');
  if (channelButton) selectChannel(channelButton.dataset.channel || channelButton.dataset.select);
  const rangeButton = event.target.closest('[data-range]');
  if (rangeButton) {
    state.range = Number(rangeButton.dataset.range);
    document.querySelectorAll('[data-range]').forEach(button => button.setAttribute('aria-pressed', String(button === rangeButton)));
    renderChart(); announce(`최근 ${state.range}분 추이를 표시합니다.`);
  }
  if (event.target.closest('[data-open-info]')) document.querySelector('#info-dialog').showModal();
  if (event.target.closest('.dialog-close')) document.querySelector('#channel-dialog').close();
});
document.querySelector('#channel-search').addEventListener('input', event => {
  navigatePlatform('search', {q:event.target.value}, {replace:state.view === 'search',scroll:false});
});
document.addEventListener('keydown', event => {
  if (event.key === '/' && !event.ctrlKey && !event.metaKey && !event.altKey && !event.target.closest('input,textarea,select,[contenteditable="true"]') && !document.querySelector('dialog[open]')) {
    event.preventDefault(); document.querySelector('#channel-search').focus();
  }
});
document.querySelector('#reset-filters').addEventListener('click', resetFilters);
document.querySelector('#watch-button').addEventListener('click', () => { document.querySelector('#watch-note').hidden = false; });
document.querySelector('#coverage-toggle').addEventListener('click', event => {
  const button = event.currentTarget;
  const expanded = button.getAttribute('aria-expanded') === 'true';
  button.setAttribute('aria-expanded', String(!expanded));
  document.querySelector('#coverage-panel').hidden = expanded;
});
document.querySelectorAll('dialog').forEach(dialog => dialog.addEventListener('click', event => {
  if (event.target === event.currentTarget) {
    const rect = event.currentTarget.getBoundingClientRect();
    if (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom) event.currentTarget.close();
  }
}));
new ResizeObserver(() => { if (document.querySelector('#channel-dialog').open) renderChart(); }).observe(document.querySelector('#chart'));
renderPlatformNavigation(); renderSurges(); applyPlatformRoute({scroll:false});
