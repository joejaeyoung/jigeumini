import { Component, StrictMode, type ErrorInfo, type ReactNode } from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter } from 'react-router';
import { App } from './app/App';
import './styles/site.css';
class ErrorBoundary extends Component<{ children: ReactNode }, { failed: boolean }> {
  state = { failed: false };
  static getDerivedStateFromError() { return { failed: true }; }
  componentDidCatch(_error: Error, _info: ErrorInfo) { /* 응답·식별자를 콘솔에 기록하지 않는다. */ }
  render() {
    return this.state.failed ? <main className="container"><section className="empty-state"><h1>화면을 불러오지 못했어요</h1><p>잠시 후 다시 시도해주세요.</p><button className="primary-button" onClick={() => window.location.reload()}>새로고침</button></section></main> : this.props.children;
  }
}
createRoot(document.getElementById('root')!).render(<StrictMode><ErrorBoundary><BrowserRouter><App/></BrowserRouter></ErrorBoundary></StrictMode>);
