export function generateSessionId(): string {
  if (typeof crypto !== 'undefined' && crypto.randomUUID) {
    return crypto.randomUUID();
  }
  const seg = () => Math.random().toString(36).slice(2, 10);
  return seg() + seg();
}
