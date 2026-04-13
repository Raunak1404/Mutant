import { useCallback, useEffect, useRef, useState } from 'react';
import { useAppState, proposalFingerprint } from './hooks/useAppState';
import { useJobStream } from './hooks/useJobStream';
import { streamChatMessage, fetchChatHistory, confirmChanges } from './api/chat';
import { uploadFiles, submitFeedback } from './api/jobs';
import { markFeedbackSubmitted } from './lib/feedbackSuppress';
import { downloadJobResult } from './lib/download';
import { Header } from './components/Header';
import { ChatView } from './components/ChatView';
import { ChatInput } from './components/ChatInput';
import { Toast } from './components/Toast';
import type { ChatMessage, UserFeedback } from './types';

export default function App() {
  const { state, dispatch, showToast } = useAppState();
  const { connect, disconnect } = useJobStream(state.jobId, dispatch, showToast);
  const chatEndRef = useRef<HTMLDivElement>(null);
  const sendingRef = useRef(false);
  const [historyLoaded, setHistoryLoaded] = useState(false);

  // ── Load chat history on mount ────────────────────────────────────
  useEffect(() => {
    if (historyLoaded) return;
    let cancelled = false;

    fetchChatHistory(state.chatSessionId).then((items) => {
      if (cancelled) return;
      setHistoryLoaded(true);

      if (items.length === 0) {
        dispatch({
          type: 'SET_CHAT_MESSAGES',
          messages: [{
            id: 'welcome',
            role: 'assistant',
            content:
              'Welcome to Mutant! Upload your SAP and eSJC Excel files below to get started, or ask me anything about your data processing pipeline.',
            timestamp: Date.now(),
          }],
        });
        return;
      }
      const messages: ChatMessage[] = items.map((item, i) => ({
        id: `history-${i}`,
        role: item.role as ChatMessage['role'],
        content: item.content,
        proposed_changes: item.proposed_changes,
        needs_confirmation: item.needs_confirmation,
        questions: item.questions,
        timestamp: new Date(item.created_at).getTime(),
      }));
      dispatch({ type: 'SET_CHAT_MESSAGES', messages });
    });

    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [historyLoaded]);

  // ── Auto-scroll on updates ────────────────────────────────────────
  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [state.chatMessages.length, state.isStreaming, state.streamingText]);

  // ── Send chat message (with SSE streaming) ────────────────────────
  const handleSendMessage = useCallback(
    async (text: string) => {
      if (sendingRef.current || !text.trim()) return;
      sendingRef.current = true;

      const userMsg: ChatMessage = {
        id: `user-${Date.now()}`,
        role: 'user',
        content: text.trim(),
        timestamp: Date.now(),
      };
      dispatch({ type: 'ADD_CHAT_MESSAGE', message: userMsg });
      dispatch({ type: 'START_STREAMING' });

      try {
        const result = await streamChatMessage(state.chatSessionId, text.trim(), {
          onThinking: (label) => dispatch({ type: 'SET_STREAMING_LABEL', label }),
          onDelta: (delta) => dispatch({ type: 'APPEND_STREAMING_TEXT', text: delta }),
          onResult: (res) => {
            dispatch({ type: 'CLEAR_STREAMING' });
            dispatch({
              type: 'ADD_CHAT_MESSAGE',
              message: {
                id: `assistant-${Date.now()}`,
                role: 'assistant',
                content: res.message,
                proposed_changes: res.proposed_changes,
                needs_confirmation: res.needs_confirmation,
                questions: res.questions,
                applied_proposals: res.applied_proposals,
                timestamp: Date.now(),
              },
            });

            if (res.applied_proposals?.length) {
              dispatch({
                type: 'RECORD_APPLIED_PROPOSALS',
                fingerprints: res.applied_proposals.map(proposalFingerprint),
              });
            }
          },
        });

        if (!result) {
          dispatch({ type: 'CLEAR_STREAMING' });
        }
      } catch (err) {
        dispatch({ type: 'CLEAR_STREAMING' });
        showToast(`Chat error: ${err instanceof Error ? err.message : 'Unknown error'}`, 'error');
      } finally {
        sendingRef.current = false;
      }
    },
    [state.chatSessionId, dispatch, showToast],
  );

  // ── Upload files & start pipeline ─────────────────────────────────
  const handleProcess = useCallback(async () => {
    if (!state.sapFile || !state.esjcFile) {
      showToast('Please upload both SAP and eSJC files', 'error');
      return;
    }

    dispatch({ type: 'SET_PIPELINE_STATUS', status: 'uploading' });
    dispatch({
      type: 'ADD_CHAT_MESSAGE',
      message: {
        id: `system-upload-${Date.now()}`,
        role: 'system',
        content: 'Uploading files and starting processing...',
        timestamp: Date.now(),
      },
    });

    try {
      const response = await uploadFiles(state.sapFile, state.esjcFile);
      dispatch({ type: 'SET_JOB_ID', jobId: response.job_id });
      dispatch({ type: 'SET_PIPELINE_STATUS', status: 'running' });
      showToast('Files uploaded, processing started', 'success');
    } catch (err) {
      dispatch({ type: 'SET_PIPELINE_STATUS', status: 'failed' });
      showToast(`Upload failed: ${err instanceof Error ? err.message : 'Unknown error'}`, 'error');
    }
  }, [state.sapFile, state.esjcFile, dispatch, showToast]);

  // ── Submit feedback ───────────────────────────────────────────────
  const handleFeedbackSubmit = useCallback(
    async (answers: UserFeedback[]) => {
      if (!state.jobId) return;

      dispatch({
        type: 'ADD_CHAT_MESSAGE',
        message: {
          id: `system-feedback-${Date.now()}`,
          role: 'system',
          content: 'Submitting feedback and resuming processing...',
          timestamp: Date.now(),
        },
      });

      try {
        await submitFeedback(state.jobId, answers);
        markFeedbackSubmitted(state.jobId);
        dispatch({ type: 'SET_QUESTIONS', questions: [] });
        dispatch({ type: 'SET_PIPELINE_STATUS', status: 'running' });
        connect(state.jobId);
        showToast('Feedback submitted, processing resumed', 'success');
      } catch (err) {
        dispatch({ type: 'SET_PIPELINE_STATUS', status: 'feedback' });
        showToast(
          `Feedback failed: ${err instanceof Error ? err.message : 'Unknown error'}`,
          'error',
        );
        throw err; // Re-throw so ChatView can detect failure and restore the form
      }
    },
    [state.jobId, dispatch, connect, showToast],
  );

  // ── Apply proposed changes ────────────────────────────────────────
  const handleConfirm = useCallback(async () => {
    try {
      const result = await confirmChanges(state.chatSessionId, state.jobId);
      dispatch({
        type: 'ADD_CHAT_MESSAGE',
        message: {
          id: `assistant-confirm-${Date.now()}`,
          role: 'assistant',
          content: result.message,
          applied_proposals: result.applied_proposals,
          timestamp: Date.now(),
        },
      });

      if (result.applied_proposals?.length) {
        dispatch({
          type: 'RECORD_APPLIED_PROPOSALS',
          fingerprints: result.applied_proposals.map(proposalFingerprint),
        });
        showToast('Changes applied successfully', 'success');
      }
    } catch (err) {
      showToast(`Apply failed: ${err instanceof Error ? err.message : 'Unknown error'}`, 'error');
      throw err; // Re-throw so ProposedChanges can detect failure and restore the button
    }
  }, [state.chatSessionId, state.jobId, dispatch, showToast]);

  // ── Download result ZIP ───────────────────────────────────────────
  const handleDownload = useCallback(async () => {
    if (!state.jobId) return;
    try {
      const result = await downloadJobResult(state.jobId);
      showToast(result.message, 'success');
    } catch (err) {
      showToast(
        `Download failed: ${err instanceof Error ? err.message : 'Unknown error'}`,
        'error',
      );
    }
  }, [state.jobId, showToast]);

  // ── Reset pipeline for a new job ──────────────────────────────────
  const handleReset = useCallback(() => {
    dispatch({ type: 'RESET_PIPELINE' });
    dispatch({
      type: 'ADD_CHAT_MESSAGE',
      message: {
        id: `system-reset-${Date.now()}`,
        role: 'system',
        content: 'Pipeline reset. Upload new files to start another job.',
        timestamp: Date.now(),
      },
    });
  }, [dispatch]);

  // ── Cancel processing & allow re-upload ───────────────────────────
  const handleCancelProcessing = useCallback(() => {
    // 1. Disconnect the WebSocket so we stop receiving events
    disconnect();

    // 2. Clear uploaded files so the user can re-upload fresh ones
    dispatch({ type: 'SET_SAP_FILE', file: null });
    dispatch({ type: 'SET_ESJC_FILE', file: null });

    // 3. Reset the pipeline state
    dispatch({ type: 'RESET_PIPELINE' });

    // 4. Inform user
    dispatch({
      type: 'ADD_CHAT_MESSAGE',
      message: {
        id: `system-cancel-${Date.now()}`,
        role: 'system',
        content: 'Processing cancelled. You can upload new files and try again.',
        timestamp: Date.now(),
      },
    });
    showToast('Processing cancelled', 'info');
  }, [disconnect, dispatch, showToast]);

  // ── Render ────────────────────────────────────────────────────────
  return (
    <div className="app-shell">
      <Header />
      <ChatView
        messages={state.chatMessages}
        isStreaming={state.isStreaming}
        streamingText={state.streamingText}
        streamingLabel={state.streamingLabel}
        pipelineStatus={state.pipelineStatus}
        steps={state.steps}
        questions={state.questions}
        jobId={state.jobId}
        appliedProposals={state.appliedProposals}
        onConfirm={handleConfirm}
        onFeedbackSubmit={handleFeedbackSubmit}
        onDownload={handleDownload}
        onReset={handleReset}
        chatEndRef={chatEndRef}
      />
      <ChatInput
        onSend={handleSendMessage}
        disabled={state.isStreaming}
        sapFile={state.sapFile}
        esjcFile={state.esjcFile}
        onSapFile={(f) => dispatch({ type: 'SET_SAP_FILE', file: f })}
        onEsjcFile={(f) => dispatch({ type: 'SET_ESJC_FILE', file: f })}
        canProcess={!!state.sapFile && !!state.esjcFile && state.pipelineStatus === 'idle'}
        onProcess={handleProcess}
        pipelineStatus={state.pipelineStatus}
        onCancelProcessing={handleCancelProcessing}
      />
      <Toast toasts={state.toasts} />
    </div>
  );
}
