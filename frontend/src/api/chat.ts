import type { ChatResponse, ChatHistoryItem, ProposedChange } from '../types';

interface StreamCallbacks {
  onStart?: () => void;
  onThinking?: (label: string) => void;
  onDelta?: (text: string) => void;
  onResult?: (result: ChatResponse) => void;
}

export async function streamChatMessage(
  sessionId: string,
  message: string,
  callbacks: StreamCallbacks,
  signal?: AbortSignal,
): Promise<ChatResponse | null> {
  const response = await fetch('/chat/message/stream', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ session_id: sessionId, message }),
    signal,
  });

  if (!response.ok || !response.body) {
    const errorText = await response.text().catch(() => 'Unknown error');
    throw new Error(`Chat request failed: ${response.status} ${errorText}`);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let resultPayload: ChatResponse | null = null;

  while (true) {
    const { value, done } = await reader.read();
    buffer += decoder.decode(value ?? new Uint8Array(), { stream: !done });

    let sepMatch = buffer.match(/\r?\n\r?\n/);
    while (sepMatch) {
      const sepIndex = sepMatch.index!;
      const block = buffer.slice(0, sepIndex);
      buffer = buffer.slice(sepIndex + sepMatch[0].length);

      const parsed = parseSSEBlock(block);
      if (parsed) {
        switch (parsed.event) {
          case 'start':
            callbacks.onStart?.();
            break;
          case 'thinking':
            callbacks.onThinking?.(parsed.data?.label ?? 'Thinking');
            break;
          case 'delta':
            callbacks.onDelta?.(parsed.data?.text ?? '');
            break;
          case 'result':
            resultPayload = parsed.data as ChatResponse;
            callbacks.onResult?.(resultPayload);
            break;
        }
      }
      sepMatch = buffer.match(/\r?\n\r?\n/);
    }

    if (done) break;
  }

  // Handle any remaining buffer
  if (buffer.trim()) {
    const parsed = parseSSEBlock(buffer);
    if (parsed?.event === 'result') {
      resultPayload = parsed.data as ChatResponse;
      callbacks.onResult?.(resultPayload);
    }
  }

  return resultPayload;
}

function parseSSEBlock(block: string): { event: string; data: any } | null {
  if (!block.trim()) return null;

  let eventName = 'message';
  const dataLines: string[] = [];

  for (const rawLine of block.split('\n')) {
    const line = rawLine.replace(/\r$/, '');
    if (line.startsWith('event:')) {
      eventName = line.slice(6).trim();
    } else if (line.startsWith('data:')) {
      dataLines.push(line.slice(5).trimStart());
    }
  }

  const dataText = dataLines.join('\n');
  if (!dataText) return { event: eventName, data: null };

  try {
    return { event: eventName, data: JSON.parse(dataText) };
  } catch {
    return { event: eventName, data: dataText };
  }
}

export async function fetchChatHistory(sessionId: string): Promise<ChatHistoryItem[]> {
  const response = await fetch(`/chat/history?session_id=${encodeURIComponent(sessionId)}`);
  if (!response.ok) return [];

  const items: ChatHistoryItem[] = await response.json();

  // Enrich with metadata (replaces the old IIFE fetch-proxy enrichment)
  return items.map((item) => {
    if (!item.metadata_json || item.metadata_json === '{}') return item;
    try {
      const meta = JSON.parse(item.metadata_json) as {
        proposed_changes?: ProposedChange[];
        proposal_status?: string;
        questions?: string[];
      };
      if (meta.proposed_changes?.length) {
        item.proposed_changes = meta.proposed_changes;
        item.needs_confirmation = meta.proposal_status === 'pending';
      }
      if (meta.questions?.length) {
        item.questions = meta.questions;
      }
    } catch {
      /* ignore malformed metadata */
    }
    return item;
  });
}

export async function confirmChanges(
  sessionId: string,
  jobId?: string | null,
): Promise<ChatResponse> {
  const response = await fetch('/chat/confirm', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ session_id: sessionId, job_id: jobId ?? null }),
  });
  if (!response.ok) {
    const detail = await response.text().catch(() => 'Unknown error');
    throw new Error(`Confirm failed: ${response.status} ${detail}`);
  }
  return response.json();
}
