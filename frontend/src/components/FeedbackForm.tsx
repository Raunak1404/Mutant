import { useState, useCallback } from 'react';
import type { FeedbackQuestion, UserFeedback } from '../types';

interface Props {
  questions: FeedbackQuestion[];
  onSubmit: (answers: UserFeedback[]) => void;
  submitting: boolean;
}

export function FeedbackForm({ questions, onSubmit, submitting }: Props) {
  const [selectedSuggestions, setSelectedSuggestions] = useState<Record<string, string | null>>({});
  const [textAnswers, setTextAnswers] = useState<Record<string, string>>({});
  const [customMode, setCustomMode] = useState<Record<string, boolean>>({});

  const handleSubmit = useCallback(() => {
    const answers: UserFeedback[] = questions.map((q) => ({
      question_id: q.question_id,
      answer: textAnswers[q.question_id] || 'Acknowledged',
    }));
    onSubmit(answers);
  }, [questions, textAnswers, onSubmit]);

  const handleBackToSuggestions = (qId: string) => {
    setCustomMode((c) => ({ ...c, [qId]: false }));
    setTextAnswers((t) => {
      const next = { ...t };
      delete next[qId];
      return next;
    });
    setSelectedSuggestions((s) => {
      const next = { ...s };
      delete next[qId];
      return next;
    });
  };


  const selectSuggestion = (qId: string, suggestion: { suggestion_id: string; description: string }) => {
    setSelectedSuggestions((s) => ({ ...s, [qId]: suggestion.suggestion_id }));
    setTextAnswers((t) => ({ ...t, [qId]: suggestion.description }));
    setCustomMode((c) => ({ ...c, [qId]: false }));
  };

  return (
    <div className="msg-row msg-system">
      <div className="feedback-card">
        <div className="feedback-header">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <circle cx="12" cy="12" r="10" />
            <path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3" />
            <line x1="12" y1="17" x2="12.01" y2="17" />
          </svg>
          <span>Feedback Required</span>
        </div>

        {questions.map((q) => (
          <div key={q.question_id} className="feedback-question">
            <div className="fq-step">Step {q.step_number}</div>
            <div className="fq-text">{q.question_text}</div>

            {q.analysis_summary && (
              <div className="fq-analysis">{q.analysis_summary}</div>
            )}

            {q.failure_pattern && (
              <div className="fq-pattern">
                <code>{q.failure_pattern}</code>
              </div>
            )}

            {q.suggestions.length > 0 && !customMode[q.question_id] && (
              <div className="fq-suggestions">
                {q.suggestions.map((s) => (
                  <label
                    key={s.suggestion_id}
                    className={`fq-suggestion ${selectedSuggestions[q.question_id] === s.suggestion_id ? 'fq-suggestion-selected' : ''}`}
                  >
                    <input
                      type="radio"
                      name={`suggestion-${q.question_id}`}
                      checked={selectedSuggestions[q.question_id] === s.suggestion_id}
                      onChange={() => selectSuggestion(q.question_id, s)}
                    />
                    <div>
                      <div className="fq-suggestion-label">{s.label}</div>
                      <div className="fq-suggestion-desc">{s.description}</div>
                    </div>
                  </label>
                ))}
                <button
                  className="btn btn-text"
                  onClick={() => setCustomMode((c) => ({ ...c, [q.question_id]: true }))}
                >
                  Provide custom instructions
                </button>
              </div>
            )}

            {(q.suggestions.length === 0 || customMode[q.question_id]) && (
              <div className="fq-custom-block">
                {q.suggestions.length > 0 && customMode[q.question_id] && (
                  <button
                    className="btn btn-text fq-back-btn"
                    onClick={() => handleBackToSuggestions(q.question_id)}
                  >
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <path d="M19 12H5" />
                      <polyline points="12 19 5 12 12 5" />
                    </svg>
                    Back to suggestions
                  </button>
                )}
                <textarea
                  className="fq-textarea"
                  placeholder="Type your instructions..."
                  value={textAnswers[q.question_id] || ''}
                  onChange={(e) => setTextAnswers((t) => ({ ...t, [q.question_id]: e.target.value }))}
                  rows={3}
                />
              </div>
            )}
          </div>
        ))}

        <button
          className="btn btn-primary feedback-submit"
          onClick={handleSubmit}
          disabled={submitting}
        >
          {submitting ? (
            <>
              <span className="spinner" />
              Submitting...
            </>
          ) : (
            'Submit Feedback'
          )}
        </button>
      </div>
    </div>
  );
}
