import { useEffect, useRef, type ReactNode } from 'react';
export function Modal({ title, onClose, children }: { title: string; onClose: () => void; children: ReactNode }) {
  const ref = useRef<HTMLDialogElement>(null);
  const onCloseRef = useRef(onClose);
  onCloseRef.current = onClose;
  useEffect(() => {
    const dialog = ref.current!;
    const trigger = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const oldOverflow = document.body.style.overflowY;
    document.body.style.overflowY = 'hidden';
    dialog.showModal();
    return () => {
      dialog.close(); document.body.style.overflowY = oldOverflow;
      if (trigger?.isConnected) trigger.focus({ preventScroll: true });
    };
  }, []);
  return <dialog ref={ref} aria-labelledby="modal-heading" onCancel={event => { event.preventDefault(); onCloseRef.current(); }}>
    <header className="modal-header"><h2 id="modal-heading">{title}</h2><button type="button" aria-label="닫기" onClick={onClose}>✕</button></header>{children}
  </dialog>;
}
