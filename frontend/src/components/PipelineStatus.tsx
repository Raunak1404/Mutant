import type { PipelineStep, PipelineStatus as PStatus } from '../types';

interface Props {
  steps: PipelineStep[];
  status: PStatus;
  onReset?: () => void;
}

export function PipelineStatus({ steps, status, onReset }: Props) {
  if (status === 'idle') return null;

  return (
    <div className="msg-row msg-system">
      <div className="pipeline-card">
        <div className="pipeline-header">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />
          </svg>
          <span>Pipeline Progress</span>
          {status === 'running' && <span className="pipeline-badge pipeline-badge-running">Running</span>}
          {status === 'completed' && <span className="pipeline-badge pipeline-badge-done">Complete</span>}
          {status === 'failed' && <span className="pipeline-badge pipeline-badge-failed">Failed</span>}
          {status === 'feedback' && <span className="pipeline-badge pipeline-badge-feedback">Awaiting Feedback</span>}
          {status === 'uploading' && <span className="pipeline-badge pipeline-badge-running">Uploading</span>}
        </div>
        <div className="pipeline-steps">
          {steps.map((step) => (
            <div key={step.number} className={`pipeline-step step-${step.state}`}>
              <div className="step-dot-wrapper">
                <div className={`step-dot dot-${step.state}`} />
              </div>
              <span className="step-name">Step {step.number}: {step.name}</span>
              <span className={`step-status status-${step.state}`}>
                {step.state === 'done' ? 'Done' : step.state === 'running' ? 'Running' : step.state === 'failed' ? 'Failed' : step.state === 'feedback' ? 'Feedback' : ''}
              </span>
            </div>
          ))}
        </div>
        {status === 'failed' && onReset && (
          <button className="btn btn-text" style={{ marginTop: '0.5rem' }} onClick={onReset}>
            Start over with new files
          </button>
        )}
      </div>
    </div>
  );
}
