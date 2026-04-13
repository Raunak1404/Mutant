import type { ToastItem } from '../types';

interface Props {
  toasts: ToastItem[];
}

export function Toast({ toasts }: Props) {
  if (!toasts.length) return null;

  return (
    <div className="toast-container">
      {toasts.map((t) => (
        <div key={t.id} className={`toast toast-${t.type}`}>
          {t.message}
        </div>
      ))}
    </div>
  );
}
