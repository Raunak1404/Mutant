import type { RefObject } from 'react';
import type {
  ChatMessage,
  PipelineStep,
  PipelineStatus as PStatus,
  FeedbackQuestion,
  UserFeedback,
} from '../types';
import { MessageBubble } from './MessageBubble';
import { StreamingBubble } from './StreamingBubble';
import { PipelineStatus } from './PipelineStatus';
import { FeedbackForm } from './FeedbackForm';
import { CompletionCard } from './CompletionCard';
import { useState, useRef, useMemo, useEffect } from 'react';

interface Props {
  messages: ChatMessage[];
  isStreaming: boolean;
  streamingText: string;
  streamingLabel: string;
  pipelineStatus: PStatus;
  steps: PipelineStep[];
  questions: FeedbackQuestion[];
  jobId: string | null;
  appliedProposals: Set<string>;
  onConfirm: () => Promise<void> | void;
  onFeedbackSubmit: (answers: UserFeedback[]) => void | Promise<void>;
  onDownload: () => void;
  onReset: () => void;
  chatEndRef: RefObject<HTMLDivElement | null>;
}

export function ChatView({
  messages,
  isStreaming,
  streamingText,
  streamingLabel,
  pipelineStatus,
  steps,
  questions,
  appliedProposals,
  onConfirm,
  onFeedbackSubmit,
  onDownload,
  onReset,
  chatEndRef,
}: Props) {
  // Track which question IDs the user has already submitted answers for.
  // Stored in a ref so it is immune to re-renders, prop changes, and
  // WebSocket event replays — once submitted, it stays submitted.
  const submittedIdsRef = useRef<Set<string>>(new Set());
  const [downloading, setDownloading] = useState(false);

  // Throwaway counter: bumped after mutating the ref so useMemo recalculates.
  const [dismissTick, setDismissTick] = useState(0);

  // Clear submitted IDs when the pipeline resets or completes so stale IDs
  // from a previous job never suppress a future feedback form.
  useEffect(() => {
    if (pipelineStatus === 'idle' || pipelineStatus === 'completed') {
      submittedIdsRef.current.clear();
    }
  }, [pipelineStatus]);

  // Derive visibility: show the form only when there are un-submitted questions.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const showFeedbackForm = useMemo(() => {
    if (pipelineStatus !== 'feedback' || questions.length === 0) return false;
    return questions.some((q) => !submittedIdsRef.current.has(q.question_id));
  }, [pipelineStatus, questions, dismissTick]);

  const handleFeedback = (answers: UserFeedback[]) => {
    // Immediately record every question ID as submitted — this is synchronous
    // and stored in a ref, so no async race can undo it.
    const qIds = questions.map((q) => q.question_id);
    for (const id of qIds) {
      submittedIdsRef.current.add(id);
    }

    // Bump tick to force useMemo to recalculate → showFeedbackForm becomes false.
    // React processes this state update synchronously before yielding to the
    // browser, so the form disappears in the same frame as the click.
    setDismissTick((t) => t + 1);

    // Fire-and-forget the parent handler. If the API call fails, the parent
    // re-throws and we catch it here to restore the form for retry.
    Promise.resolve(onFeedbackSubmit(answers)).catch(() => {
      for (const id of qIds) {
        submittedIdsRef.current.delete(id);
      }
      setDismissTick((t) => t + 1);
    });
  };

  const handleDownload = async () => {
    setDownloading(true);
    try {
      await onDownload();
    } finally {
      setDownloading(false);
    }
  };

  return (
    <div className="chat-view">
      <div className="chat-messages">
        {messages.map((msg) => (
          <MessageBubble
            key={msg.id}
            message={msg}
            appliedProposals={appliedProposals}
            onConfirm={onConfirm}
          />
        ))}

        {/* Pipeline progress inline */}
        {pipelineStatus !== 'idle' && (
          <PipelineStatus steps={steps} status={pipelineStatus} onReset={onReset} />
        )}

        {/* Feedback form inline */}
        {showFeedbackForm && (
          <FeedbackForm
            questions={questions}
            onSubmit={handleFeedback}
            submitting={false}
          />
        )}

        {/* Streaming indicator */}
        {isStreaming && (
          <StreamingBubble label={streamingLabel} text={streamingText} />
        )}

        {/* Completion card */}
        {pipelineStatus === 'completed' && (
          <CompletionCard onDownload={handleDownload} downloading={downloading} onReset={onReset} />
        )}

        <div ref={chatEndRef} />
      </div>
    </div>
  );
}
