import { useEffect, useRef, useCallback } from 'react';
import type { WSEvent } from '../types';
import type { Action } from './useAppState';
import { INITIAL_STEPS } from './useAppState';
import { fetchQuestions } from '../api/jobs';
import {
  shouldSuppressAwaitingFeedback,
  consumeFeedbackSuppression,
  onStepStartedForJob,
} from '../lib/feedbackSuppress';

export function useJobStream(
  jobId: string | null,
  dispatch: React.Dispatch<Action>,
  showToast: (msg: string, type: 'success' | 'error' | 'info') => void,
) {
  const wsRef = useRef<WebSocket | null>(null);
  const terminalRef = useRef(false);

  // Stable ref for the event handler so it sees latest closure values
  const handleEvent = useCallback(
    (event: WSEvent, activeJobId: string) => {
      const { event_type, step_number, message } = event;

      switch (event_type) {
        case 'step_started':
          onStepStartedForJob(activeJobId);
          if (step_number != null) {
            dispatch({ type: 'SET_STEP_STATE', step: step_number, state: 'running', status: message });
          }
          dispatch({ type: 'SET_PIPELINE_STATUS', status: 'running' });
          break;

        case 'step_completed':
          if (step_number != null) {
            dispatch({ type: 'SET_STEP_STATE', step: step_number, state: 'done', status: 'Completed' });
          }
          break;

        case 'awaiting_feedback':
          if (shouldSuppressAwaitingFeedback(activeJobId)) {
            consumeFeedbackSuppression();
            return;
          }
          dispatch({ type: 'SET_PIPELINE_STATUS', status: 'feedback' });
          fetchQuestions(activeJobId)
            .then((questions) => dispatch({ type: 'SET_QUESTIONS', questions }))
            .catch((err) => showToast(`Failed to load questions: ${err.message}`, 'error'));
          break;

        case 'completed':
          terminalRef.current = true;
          consumeFeedbackSuppression();
          for (const s of INITIAL_STEPS) {
            dispatch({ type: 'SET_STEP_STATE', step: s.number, state: 'done', status: 'Completed' });
          }
          dispatch({ type: 'SET_PIPELINE_STATUS', status: 'completed' });
          dispatch({
            type: 'ADD_CHAT_MESSAGE',
            message: {
              id: `system-complete-${Date.now()}`,
              role: 'system',
              content: 'Processing complete! Your output files are ready for download.',
              timestamp: Date.now(),
            },
          });
          showToast('Processing complete!', 'success');
          break;

        case 'failed': {
          terminalRef.current = true;
          consumeFeedbackSuppression();
          if (step_number != null) {
            dispatch({ type: 'SET_STEP_STATE', step: step_number, state: 'failed', status: 'Failed' });
          }
          dispatch({ type: 'SET_PIPELINE_STATUS', status: 'failed' });
          const errMsg = (event.data?.error as string) ?? message;
          showToast(`Processing failed: ${errMsg}`, 'error');
          break;
        }
      }
    },
    [dispatch, showToast],
  );

  const handleEventRef = useRef(handleEvent);
  handleEventRef.current = handleEvent;

  const connect = useCallback(
    (targetJobId: string) => {
      if (wsRef.current) {
        wsRef.current.close(1000, 'reconnect');
        wsRef.current = null;
      }
      terminalRef.current = false;

      const protocol = location.protocol === 'https:' ? 'wss' : 'ws';
      const ws = new WebSocket(`${protocol}://${location.host}/jobs/${targetJobId}/stream`);

      ws.onmessage = (ev) => {
        try {
          const event: WSEvent = JSON.parse(ev.data);
          handleEventRef.current(event, targetJobId);
        } catch {
          /* ignore parse errors */
        }
      };

      ws.onerror = () => {
        if (!terminalRef.current) {
          showToast('Live progress connection dropped', 'error');
        }
      };

      ws.onclose = (ev) => {
        if (ev.code !== 1000 && ev.code !== 1001 && !terminalRef.current) {
          showToast('Connection closed unexpectedly', 'error');
        }
      };

      wsRef.current = ws;
    },
    [showToast],
  );

  // Auto-connect when jobId changes
  useEffect(() => {
    if (jobId) {
      connect(jobId);
    }
    return () => {
      if (wsRef.current) {
        wsRef.current.close(1000, 'cleanup');
        wsRef.current = null;
      }
    };
  }, [jobId, connect]);

  const disconnect = useCallback(() => {
    terminalRef.current = true; // suppress stale error toasts
    if (wsRef.current) {
      wsRef.current.close(1000, 'user-cancel');
      wsRef.current = null;
    }
  }, []);

  return { connect, disconnect };
}
