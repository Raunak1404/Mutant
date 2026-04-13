// ── API Response Types ──────────────────────────────────────────────

export interface ProposedChange {
  step_number: number;
  change_type: 'rule' | 'code';
  description: string;
}

export interface ChatResponse {
  session_id: string;
  message: string;
  proposed_changes: ProposedChange[];
  needs_confirmation: boolean;
  questions: string[];
  applied_proposals: ProposedChange[];
}

export interface ChatHistoryItem {
  role: 'user' | 'assistant';
  content: string;
  metadata_json: string;
  created_at: string;
  proposed_changes?: ProposedChange[];
  needs_confirmation?: boolean;
  questions?: string[];
}

export interface JobUploadResponse {
  job_id: string;
  message: string;
  sap_storage_key: string;
  esjc_storage_key: string;
}

export interface FeedbackSuggestion {
  suggestion_id: string;
  label: string;
  description: string;
}

export interface FeedbackQuestion {
  question_id: string;
  step_number: number;
  question_text: string;
  failure_pattern: string;
  example_rows: Record<string, unknown>[];
  suggestions: FeedbackSuggestion[];
  analysis_summary: string;
}

export interface FeedbackSubmitResponse {
  job_id: string;
  message: string;
}

export interface UserFeedback {
  question_id: string;
  answer: string;
}

// ── WebSocket Event Types ───────────────────────────────────────────

export interface WSEvent {
  job_id: string;
  event_type: 'step_started' | 'step_completed' | 'awaiting_feedback' | 'completed' | 'failed';
  step_number: number | null;
  message: string;
  data: Record<string, unknown>;
}

// ── App State Types ─────────────────────────────────────────────────

export type PipelineStatus = 'idle' | 'uploading' | 'running' | 'feedback' | 'completed' | 'failed';

export interface PipelineStep {
  number: number;
  name: string;
  state: 'pending' | 'running' | 'done' | 'failed' | 'feedback';
  status: string;
}

export interface ChatMessage {
  id: string;
  role: 'user' | 'assistant' | 'system';
  content: string;
  proposed_changes?: ProposedChange[];
  needs_confirmation?: boolean;
  questions?: string[];
  applied_proposals?: ProposedChange[];
  timestamp: number;
}

export interface ToastItem {
  id: string;
  message: string;
  type: 'success' | 'error' | 'info';
}
