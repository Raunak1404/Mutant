import { useCallback, useRef, useState } from 'react';

interface Props {
  label: string;
  file: File | null;
  onFile: (file: File | null) => void;
  /** When true, file cannot be removed (e.g. during active processing). */
  locked?: boolean;
}

export function FileUploadArea({ label, file, onFile, locked }: Props) {
  const inputRef = useRef<HTMLInputElement>(null);
  const [dragOver, setDragOver] = useState(false);

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setDragOver(false);
      if (locked) return;
      const f = e.dataTransfer.files[0];
      if (f && (f.name.endsWith('.xlsx') || f.name.endsWith('.xls'))) {
        onFile(f);
      }
    },
    [onFile, locked],
  );

  const handleChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const f = e.target.files?.[0];
      if (f) onFile(f);
    },
    [onFile],
  );

  if (file) {
    return (
      <div className="file-chip">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <polyline points="20 6 9 17 4 12" />
        </svg>
        <span className="file-chip-name" title={file.name}>
          {label}: {file.name}
        </span>
        {!locked && (
          <button
            className="file-chip-remove"
            onClick={() => onFile(null)}
            aria-label={`Remove ${label} file`}
            title={`Remove ${label} file`}
          >
            {/* Trash icon */}
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <polyline points="3 6 5 6 21 6" />
              <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" />
              <line x1="10" y1="11" x2="10" y2="17" />
              <line x1="14" y1="11" x2="14" y2="17" />
            </svg>
          </button>
        )}
      </div>
    );
  }

  return (
    <div
      className={`file-dropzone ${dragOver ? 'file-dropzone-active' : ''}`}
      onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
      onDragLeave={() => setDragOver(false)}
      onDrop={handleDrop}
      onClick={() => inputRef.current?.click()}
      role="button"
      tabIndex={0}
      onKeyDown={(e) => { if (e.key === 'Enter') inputRef.current?.click(); }}
      aria-label={`Upload ${label} file`}
    >
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
        <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
        <polyline points="17 8 12 3 7 8" />
        <line x1="12" y1="3" x2="12" y2="15" />
      </svg>
      <span className="dropzone-label">{label}</span>
      <span className="dropzone-hint">.xlsx / .xls</span>
      <input
        ref={inputRef}
        type="file"
        accept=".xlsx,.xls"
        onChange={handleChange}
        hidden
      />
    </div>
  );
}
