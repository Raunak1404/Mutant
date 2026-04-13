interface Props {
  label: string;
  text: string;
}

export function StreamingBubble({ label, text }: Props) {
  return (
    <div className="msg-row msg-assistant">
      <div className="msg-avatar msg-avatar-ai">M</div>
      <div className="msg-bubble bubble-ai streaming-bubble">
        <div className="streaming-label">
          {label || 'Thinking'}
          {!text && <span className="streaming-dots"><span /><span /><span /></span>}
        </div>
        {text && <div className="streaming-text">{text}</div>}
      </div>
    </div>
  );
}
