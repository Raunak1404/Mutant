import type { ChatMessage } from '../types';
import { ProposedChanges } from './ProposedChanges';

interface Props {
  message: ChatMessage;
  appliedProposals: Set<string>;
  onConfirm: () => Promise<void> | void;
}

export function MessageBubble({ message, appliedProposals, onConfirm }: Props) {
  const { role, content, proposed_changes, needs_confirmation } = message;

  if (role === 'system') {
    return (
      <div className="msg-row msg-system">
        <div className="msg-system-card">{content}</div>
      </div>
    );
  }

  const isUser = role === 'user';

  return (
    <div className={`msg-row ${isUser ? 'msg-user' : 'msg-assistant'}`}>
      {!isUser && (
        <div className="msg-avatar msg-avatar-ai">M</div>
      )}
      <div className={`msg-bubble ${isUser ? 'bubble-user' : 'bubble-ai'}`}>
        <div className="msg-content" dangerouslySetInnerHTML={{ __html: formatContent(content) }} />
        {proposed_changes && proposed_changes.length > 0 && (
          <ProposedChanges
            changes={proposed_changes}
            canApply={needs_confirmation === true}
            appliedProposals={appliedProposals}
            onConfirm={onConfirm}
          />
        )}
      </div>
      {isUser && (
        <div className="msg-avatar msg-avatar-user">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2" />
            <circle cx="12" cy="7" r="4" />
          </svg>
        </div>
      )}
    </div>
  );
}

function formatContent(text: string): string {
  // Basic markdown-like formatting: **bold**, *italic*, `code`, newlines
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(.+?)\*/g, '<em>$1</em>')
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    .replace(/\n/g, '<br/>');
}
