import { useReducer, useCallback } from 'react';
import type {
  ChatMessage,
  PipelineStep,
  PipelineStatus,
  FeedbackQuestion,
  ToastItem,
  ProposedChange,
} from '../types';
import { generateSessionId } from '../lib/session';

// ── Pipeline step definitions (matches backend) ─────────────────────

export const INITIAL_STEPS: PipelineStep[] = [
  { number: 1, name: 'Load & Clean SAP', state: 'pending', status: '' },
  { number: 2, name: 'Split & Extract Actions', state: 'pending', status: '' },
  { number: 3, name: 'Classify Defects', state: 'pending', status: '' },
  { number: 4, name: 'Format SAP Output', state: 'pending', status: '' },
  { number: 5, name: 'Process ESJC Data', state: 'pending', status: '' },
  { number: 6, name: 'Package & Export', state: 'pending', status: '' },
];

// ── State shape ─────────────────────────────────────────────────────

export interface AppState {
  sapFile: File | null;
  esjcFile: File | null;
  jobId: string | null;
  pipelineStatus: PipelineStatus;
  steps: PipelineStep[];
  questions: FeedbackQuestion[];
  chatMessages: ChatMessage[];
  chatSessionId: string;
  streamingText: string;
  streamingLabel: string;
  isStreaming: boolean;
  toasts: ToastItem[];
  appliedProposals: Set<string>;
}

// ── Actions ─────────────────────────────────────────────────────────

export type Action =
  | { type: 'SET_SAP_FILE'; file: File | null }
  | { type: 'SET_ESJC_FILE'; file: File | null }
  | { type: 'SET_JOB_ID'; jobId: string }
  | { type: 'SET_PIPELINE_STATUS'; status: PipelineStatus }
  | { type: 'SET_STEP_STATE'; step: number; state: PipelineStep['state']; status?: string }
  | { type: 'RESET_PIPELINE' }
  | { type: 'SET_QUESTIONS'; questions: FeedbackQuestion[] }
  | { type: 'ADD_CHAT_MESSAGE'; message: ChatMessage }
  | { type: 'SET_CHAT_MESSAGES'; messages: ChatMessage[] }
  | { type: 'START_STREAMING' }
  | { type: 'SET_STREAMING_LABEL'; label: string }
  | { type: 'APPEND_STREAMING_TEXT'; text: string }
  | { type: 'CLEAR_STREAMING' }
  | { type: 'ADD_TOAST'; toast: ToastItem }
  | { type: 'REMOVE_TOAST'; id: string }
  | { type: 'RECORD_APPLIED_PROPOSALS'; fingerprints: string[] };

// ── Reducer ─────────────────────────────────────────────────────────

function reducer(state: AppState, action: Action): AppState {
  switch (action.type) {
    case 'SET_SAP_FILE':
      return { ...state, sapFile: action.file };
    case 'SET_ESJC_FILE':
      return { ...state, esjcFile: action.file };
    case 'SET_JOB_ID':
      return { ...state, jobId: action.jobId };
    case 'SET_PIPELINE_STATUS':
      return { ...state, pipelineStatus: action.status };
    case 'SET_STEP_STATE':
      return {
        ...state,
        steps: state.steps.map((s) =>
          s.number === action.step
            ? { ...s, state: action.state, status: action.status ?? s.status }
            : s,
        ),
      };
    case 'RESET_PIPELINE':
      return {
        ...state,
        jobId: null,
        pipelineStatus: 'idle',
        steps: INITIAL_STEPS.map((s) => ({ ...s })),
        questions: [],
      };
    case 'SET_QUESTIONS':
      return { ...state, questions: action.questions };
    case 'ADD_CHAT_MESSAGE':
      return { ...state, chatMessages: [...state.chatMessages, action.message] };
    case 'SET_CHAT_MESSAGES':
      return { ...state, chatMessages: action.messages };
    case 'START_STREAMING':
      return { ...state, isStreaming: true, streamingText: '', streamingLabel: 'Thinking' };
    case 'SET_STREAMING_LABEL':
      return { ...state, streamingLabel: action.label };
    case 'APPEND_STREAMING_TEXT':
      return { ...state, streamingText: state.streamingText + action.text };
    case 'CLEAR_STREAMING':
      return { ...state, isStreaming: false, streamingText: '', streamingLabel: '' };
    case 'ADD_TOAST':
      return { ...state, toasts: [...state.toasts, action.toast] };
    case 'REMOVE_TOAST':
      return { ...state, toasts: state.toasts.filter((t) => t.id !== action.id) };
    case 'RECORD_APPLIED_PROPOSALS': {
      const next = new Set(state.appliedProposals);
      for (const fp of action.fingerprints) next.add(fp);
      return { ...state, appliedProposals: next };
    }
    default:
      return state;
  }
}

// ── Initial state ───────────────────────────────────────────────────

const initialState: AppState = {
  sapFile: null,
  esjcFile: null,
  jobId: null,
  pipelineStatus: 'idle',
  steps: INITIAL_STEPS.map((s) => ({ ...s })),
  questions: [],
  chatMessages: [],
  chatSessionId: generateSessionId(),
  streamingText: '',
  streamingLabel: '',
  isStreaming: false,
  toasts: [],
  appliedProposals: new Set<string>(),
};

// ── Hook ────────────────────────────────────────────────────────────

export function useAppState() {
  const [state, dispatch] = useReducer(reducer, initialState);

  const showToast = useCallback(
    (message: string, type: ToastItem['type'] = 'info') => {
      const id = Math.random().toString(36).slice(2, 10);
      dispatch({ type: 'ADD_TOAST', toast: { id, message, type } });
      setTimeout(() => dispatch({ type: 'REMOVE_TOAST', id }), 3500);
    },
    [],
  );

  return { state, dispatch, showToast };
}

// ── Helpers ─────────────────────────────────────────────────────────

export function proposalFingerprint(c: ProposedChange): string {
  return [
    c.step_number,
    c.change_type.trim().toLowerCase(),
    c.description.trim().replace(/\s+/g, ' ').toLowerCase(),
  ].join('|');
}
