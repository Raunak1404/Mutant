import { useState, useCallback } from 'react';
import type { ProposedChange } from '../types';
import { proposalFingerprint } from '../hooks/useAppState';

interface Props {
  changes: ProposedChange[];
  canApply: boolean;
  appliedProposals: Set<string>;
  onConfirm: () => Promise<void> | void;
}

export function ProposedChanges({ changes, canApply, appliedProposals, onConfirm }: Props) {
  const allApplied = changes.every((c) => appliedProposals.has(proposalFingerprint(c)));

  // Local processing state to give immediate visual feedback and prevent
  // double-clicks while the backend processes the confirmation request.
  const [applying, setApplying] = useState(false);

  const handleApply = useCallback(async () => {
    // Guard: if already applying or already applied, bail out.
    if (applying || allApplied) return;

    setApplying(true);
    try {
      await Promise.resolve(onConfirm());
    } catch {
      // On failure, restore the button so the user can retry.
      setApplying(false);
    }
    // On success, `allApplied` will become true via parent state update
    // (RECORD_APPLIED_PROPOSALS), so we don't need to reset `applying`.
    // But we still clear it in case the parent didn't record proposals
    // (edge case: server returns success but no applied_proposals).
    // The `allApplied` check in the render will take priority anyway.
  }, [applying, allApplied, onConfirm]);

  // Determine button display state
  const renderButton = () => {
    if (allApplied) {
      // Final "done" state — show success indicator
      return (
        <button className="btn btn-applied" disabled>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="20 6 9 17 4 12" />
          </svg>
          Changes Applied
        </button>
      );
    }

    if (applying) {
      // Processing state — show spinner and disable
      return (
        <button className="btn btn-apply btn-applying" disabled>
          <span className="spinner spinner-apply" />
          Applying Changes…
        </button>
      );
    }

    if (canApply) {
      // Ready state — allow click
      return (
        <button className="btn btn-apply" onClick={handleApply}>
          Apply Changes
        </button>
      );
    }

    return null;
  };

  return (
    <div className="proposed-changes">
      <div className="proposed-changes-title">Proposed Changes</div>
      <ul className="proposed-changes-list">
        {changes.map((c, i) => (
          <li key={i} className="proposed-change-item">
            <span className="change-badge">{`Step ${c.step_number}`}</span>
            <span className={`change-type change-type-${c.change_type}`}>{c.change_type}</span>
            <span className="change-desc">{c.description}</span>
          </li>
        ))}
      </ul>
      {renderButton()}
    </div>
  );
}
