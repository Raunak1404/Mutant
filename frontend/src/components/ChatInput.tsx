import { useState, useCallback, useRef } from 'react';
import type { PipelineStatus } from '../types';
import { FileUploadArea } from './FileUploadArea';

interface Props {
  onSend: (text: string) => void;
  disabled: boolean;
  sapFile: File | null;
  esjcFile: File | null;
  onSapFile: (f: File | null) => void;
  onEsjcFile: (f: File | null) => void;
  canProcess: boolean;
  onProcess: () => void;
  pipelineStatus: PipelineStatus;
  onCancelProcessing: () => void;
}

export function ChatInput({
  onSend,
  disabled,
  sapFile,
  esjcFile,
  onSapFile,
  onEsjcFile,
  canProcess,
  onProcess,
  pipelineStatus,
  onCancelProcessing,
}: Props) {
  const [text, setText] = useState('');
  const inputRef = useRef<HTMLInputElement>(null);

  // Pipeline is actively processing (uploading, running, or in feedback loop)
  const isActive =
    pipelineStatus === 'running' ||
    pipelineStatus === 'uploading' ||
    pipelineStatus === 'feedback';

  // Chat is locked during active pipeline work to prevent accidental messages
  const chatLocked = disabled || isActive;

  const handleSend = useCallback(() => {
    if (!text.trim() || chatLocked) return;
    onSend(text);
    setText('');
    inputRef.current?.focus();
  }, [text, chatLocked, onSend]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        handleSend();
      }
    },
    [handleSend],
  );

  const isProcessing = pipelineStatus === 'running' || pipelineStatus === 'uploading';

  return (
    <div className="chat-input-area">
      {/* Message input */}
      <div className="chat-input-row">
        <input
          ref={inputRef}
          type="text"
          className="chat-input"
          value={text}
          onChange={(e) => setText(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder={
            isActive
              ? 'Chat is paused while the pipeline is running…'
              : 'Ask Mutant anything about your pipeline...'
          }
          disabled={chatLocked}
          autoFocus
        />
        <button
          className="btn btn-send"
          onClick={handleSend}
          disabled={chatLocked || !text.trim()}
          aria-label="Send message"
        >
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <line x1="22" y1="2" x2="11" y2="13" />
            <polygon points="22 2 15 22 11 13 2 9 22 2" />
          </svg>
        </button>
      </div>

      {/* File uploads + process / cancel buttons */}
      <div className="chat-input-footer">
        <div className="file-upload-row">
          <FileUploadArea label="SAP File" file={sapFile} onFile={onSapFile} locked={isActive} />
          <FileUploadArea label="eSJC File" file={esjcFile} onFile={onEsjcFile} locked={isActive} />
        </div>

        {/* Action row: process button and/or cancel button */}
        <div className="action-button-row">
          {(sapFile || esjcFile || canProcess || isProcessing) && (
            <button
              className="btn btn-process"
              onClick={onProcess}
              disabled={!canProcess || isActive}
            >
              {isProcessing ? (
                <>
                  <span className="spinner" />
                  Processing...
                </>
              ) : (
                <>
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <polygon points="5 3 19 12 5 21 5 3" />
                  </svg>
                  Run Pipeline
                </>
              )}
            </button>
          )}

          {isActive && (
            <button
              className="btn btn-cancel"
              onClick={onCancelProcessing}
            >
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                <circle cx="12" cy="12" r="10" />
                <line x1="15" y1="9" x2="9" y2="15" />
                <line x1="9" y1="9" x2="15" y2="15" />
              </svg>
              Cancel Processing & Re-upload
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
