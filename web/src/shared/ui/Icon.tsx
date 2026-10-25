type IconName = 'search' | 'bookmark' | 'broadcast' | 'arrow' | 'trend';
const paths: Record<IconName, React.ReactNode> = {
  search: <><circle cx="10.5" cy="10.5" r="6.5"/><path d="m16 16 4 4"/></>,
  bookmark: <path d="M6 4h12v17l-6-4-6 4Z"/>,
  broadcast: <><rect x="3" y="5" width="18" height="14" rx="3"/><path d="m10 9 5 3-5 3Z"/></>,
  arrow: <path d="M5 12h14m-5-5 5 5-5 5"/>,
  trend: <path d="m3 17 6-6 4 4 8-10M14 5h7v7"/>,
};
export function Icon({ name }: { name: IconName }) {
  return <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">{paths[name]}</svg>;
}
