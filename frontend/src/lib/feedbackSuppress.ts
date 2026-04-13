const SUPPRESS_TTL_MS = 30_000;

let suppressState = {
  jobId: null as string | null,
  ts: 0,
  stepSeenSince: false,
};

export function markFeedbackSubmitted(jobId: string): void {
  suppressState = { jobId, ts: Date.now(), stepSeenSince: false };
}

export function shouldSuppressAwaitingFeedback(jobId: string): boolean {
  if (suppressState.jobId !== jobId) return false;
  if (suppressState.stepSeenSince) return false;
  if (Date.now() - suppressState.ts > SUPPRESS_TTL_MS) return false;
  return true;
}

export function consumeFeedbackSuppression(): void {
  suppressState = { jobId: null, ts: 0, stepSeenSince: false };
}

export function onStepStartedForJob(jobId: string): void {
  if (suppressState.jobId === jobId) {
    suppressState.stepSeenSince = true;
  }
}
