export function Header() {
  return (
    <header className="app-header">
      <div className="header-brand">
        <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <polygon points="12 2 2 7 12 12 22 7 12 2" />
          <polyline points="2 17 12 22 22 17" />
          <polyline points="2 12 12 17 22 12" />
        </svg>
        <h1>Mutant</h1>
        <span className="header-subtitle">Agentic Excel Processor</span>
      </div>
    </header>
  );
}
