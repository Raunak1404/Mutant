interface Props {
  onDownload: () => void;
  downloading: boolean;
  onReset: () => void;
}

export function CompletionCard({ onDownload, downloading, onReset }: Props) {
  return (
    <div className="msg-row msg-system">
      <div className="completion-card">
        <div className="completion-icon">
          <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" />
            <polyline points="22 4 12 14.01 9 11.01" />
          </svg>
        </div>
        <div className="completion-text">
          <strong>Processing Complete</strong>
          <span>Your SAP and ESJC output files are packaged and ready.</span>
        </div>
        <button
          className="btn btn-download"
          onClick={onDownload}
          disabled={downloading}
        >
          {downloading ? (
            <>
              <span className="spinner" />
              Downloading...
            </>
          ) : (
            <>
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
                <polyline points="7 10 12 15 17 10" />
                <line x1="12" y1="15" x2="12" y2="3" />
              </svg>
              Download ZIP
            </>
          )}
        </button>
        <button className="btn btn-text" onClick={onReset}>
          Start a new job
        </button>
      </div>
    </div>
  );
}
