from __future__ import annotations

import json
import re
from collections import defaultdict
from collections.abc import AsyncIterator
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from chat.chat_models import ChatDecision, ChatResponse, ProposedChange, StepRuleSummary
from db.code_versions import create_code_version, get_latest_code
from db.models import ChatMessage, CodeVersion, JobCheckpoint, JobStatus, RuleVersion
from db.rules import create_rule_version, get_latest_rule
from feedback.code_updater import list_functions
from llm.provider import LLMProvider
from models.messages import Message
from utils.logging import get_logger

logger = get_logger(__name__)

STEP_COUNT = 6

CHAT_RESPONSE_SYSTEM_PROMPT = """You are a rules/code advisor for an aircraft maintenance data processing pipeline (SQ A350 fleet).
The pipeline has 6 steps:
  1. Load & Clean SAP — select columns, drop nulls, filter descriptions, rename
  2. Split & Extract Actions — split corrective actions, explode rows, extract dates/license/station
  3. Classify Defects — apply keyword masks (Oxygen, Toilet, NIL, Open), categorize via np.select
  4. Format SAP Output — rearrange columns, add empty manual-entry columns, yellow highlight styling
  5. Process ESJC Data — select columns, merge into Defect/Action & Corrective Action, forward-fill
  6. Package & Export — combine SAP (step 4) + ESJC (step 5), apply final styling, produce ZIP

Respond in plain text only.
- Explain briefly what you understood from the latest user message.
- If the user is asking for a change, describe the change you would apply and the step(s) involved.
- If the user gives a direct actionable change request and the affected step can be inferred from the message and current pipeline context, do not ask clarifying questions.
- Ask at most one short targeted clarifying question, and only if the affected step or desired behavior cannot be safely inferred.
- Do not output JSON, code fences, or pseudo-UI text such as "Apply Changes".
- NEVER ask the user for confirmation to apply changes (e.g. "Should I go ahead?", "Want me to apply this?", "Shall I make this change?"). The UI provides a separate Apply Changes button for that — just describe the change you would make.
- Never say the change is already applied unless the conversation explicitly confirms that it has been applied.
- If the latest user message is just a thank-you, acknowledgement, or other casual follow-up, reply briefly and do not reopen older change requests."""

CHAT_DECISION_SYSTEM_PROMPT = """You convert the latest chat turn into structured UI metadata.
Your job is to detect when the assistant describes a change and return proposed_changes so the UI can show an "Apply Changes" button.

Rules:
- Evaluate ONLY the latest user turn. Older pending proposals are context, not new work.
- If the latest user message is casual acknowledgement, gratitude, or small talk, return no proposed changes and no questions.
- **CRITICAL**: If the assistant reply describes ANY change it would make to the pipeline rules or code (adding, removing, renaming, updating columns, aliases, filters, categories, formatting, etc.), you MUST return proposed_changes. The UI cannot show the "Apply Changes" button without them.
- When the assistant mentions specific step numbers (e.g. "Step 1", "Step 4"), use those step numbers in proposed_changes.
- Confirmation questions from the assistant are NOT clarifying questions — the UI has a separate Apply Changes button.
- Only return a clarifying question (in the questions field) when you genuinely cannot determine which step or what behavior the user wants.
- For a direct actionable change request, ALWAYS return proposed_changes when the affected step can be inferred from the user message, the assistant reply, or the current pipeline state.
- For steps that use native Python code, runtime-affecting changes should include BOTH a "rule" change AND a "code" change for that step.
- Do not resurrect or repeat previously applied proposals.
- When in doubt about whether to include proposed_changes, INCLUDE them. It is far better to show the Apply button unnecessarily than to hide it when the user expects it.

Example: if the assistant says "I would update Step 1 to remove the Aircraft Tail alias", return:
  proposed_changes: [{step_number: 1, change_type: "rule", description: "Remove Aircraft Tail alias from Tail column"}]
"""

APPLY_RULE_CHANGE_PROMPT = """You are a rules editor for an aircraft maintenance data processing pipeline.
Given the current rule file (markdown) and a description of the requested change, produce the UPDATED rule file.
Return ONLY the full updated markdown content. No explanation, no fencing."""

APPLY_CODE_CHANGE_PROMPT = """You are a Python code editor for an aircraft maintenance data processing pipeline.
Given the current Python source file, the active step rules, and a description of the requested change, produce the UPDATED full Python file.
Return ONLY the full updated Python source code. No explanation, no markdown fencing."""

ACKNOWLEDGEMENT_PATTERNS = (
    "thanks",
    "thank you",
    "thx",
    "got it",
    "okay",
    "ok",
    "cool",
    "nice",
    "great",
    "perfect",
    "understood",
    "makes sense",
    "no worries",
)

CONFIRMATION_PATTERNS = (
    "yes",
    "yep",
    "yeah",
    "yea",
    "sure",
    "go ahead",
    "do it",
    "confirm",
    "apply",
    "proceed",
    "please do",
    "yes please",
    "go for it",
    "make the change",
    "make the changes",
    "apply the change",
    "apply the changes",
    "apply it",
)

CHANGE_KEYWORDS = (
    "change",
    "update",
    "fix",
    "rule",
    "code",
    "step",
    "add",
    "remove",
    "modify",
    "adjust",
    "error",
    "issue",
)

RESTORE_KEYWORDS = (
    "restore",
    "revert",
    "undo",
    "go back",
    "move back",
    "previous behavior",
    "previous version",
    "original behavior",
    "original rules",
    "before the",
)

STEP_TITLES = {
    1: "Load & Clean SAP Data",
    2: "Split & Extract SAP Actions",
    3: "Classify SAP Defects",
    4: "Format SAP Output",
    5: "Process ESJC Data",
    6: "Package & Export",
}

STEP_HINTS = {
    1: (
        "load & clean sap",
        "clean sap",
        "sap raw data",
        "description column",
        "defect text1",
        "action text1",
        "ata code",
        "column alias",
        "aircraft tail",
        "tail column",
        "select columns",
        "drop null",
        "filter description",
        "rename column",
        "input column",
        "sap column",
        "column name",
        "alias",
    ),
    2: (
        "split corrective action",
        "split action",
        "resolved date",
        "lic no",
        "license number",
        "to station",
        "defect action",
        "action count",
        "explode row",
        "extract date",
        "extract license",
    ),
    3: (
        "classify defect",
        "classification",
        "defect category",
        "oxygen issue",
        "toilet choke",
        "nil defect",
        "open defect",
        "unclassified",
        "category",
        "keyword mask",
        "defect type",
    ),
    4: (
        "format sap output",
        "manual-entry",
        "manual entry",
        "rearrange columns",
        "column order",
        "yellow highlight",
        "empty column",
        "manual column",
        "output format",
    ),
    5: (
        "process esjc",
        "esjc",
        "detailed report",
        "forward-fill",
        "forward fill",
        "defect/action required",
        "action taken",
        "esjc column",
        "esjc data",
    ),
    6: (
        "package & export",
        "package export",
        "zip",
        "download",
        "yellow highlighting",
        "output_sap",
        "output_esjc",
        "combine output",
        "final output",
    ),
}

CHANGE_REQUEST_PATTERN = re.compile(
    r"\b("
    r"change|update|modify|adjust|fix|add|remove|include|exclude|"
    r"rename|treat|classify|reclassify|format|highlight|export|"
    r"merge|split|forward[- ]fill|drop|keep|filter"
    r")\b",
    re.IGNORECASE,
)
EXPLICIT_STEP_PATTERN = re.compile(r"\bstep\s*([1-6])\b", re.IGNORECASE)
INFO_REQUEST_PREFIXES = (
    "what ",
    "which ",
    "why ",
    "how ",
    "show ",
    "list ",
    "explain ",
    "tell me ",
)
GENERIC_MATCH_TOKENS = {
    "step",
    "data",
    "output",
    "input",
    "rules",
    "rule",
    "logic",
    "file",
    "files",
    "column",
    "columns",
}


async def load_step_context(session: AsyncSession) -> list[StepRuleSummary]:
    """Load current rules and code function names for all steps."""
    summaries = []
    for step_num in range(1, STEP_COUNT + 1):
        rule = await get_latest_rule(session, step_num)
        code = await get_latest_code(session, step_num)

        rule_content = rule.content if rule else "(no rule found)"
        code_functions: list[str] = []
        if code:
            funcs = list_functions(code.content)
            code_functions = [f["name"] for f in funcs]

        summaries.append(
            StepRuleSummary(
                step_number=step_num,
                rule_content=rule_content,
                code_functions=code_functions,
            )
        )
    return summaries


def _build_context_block(summaries: list[StepRuleSummary]) -> str:
    parts = []
    for summary in summaries:
        funcs = ", ".join(summary.code_functions) if summary.code_functions else "(none)"
        parts.append(
            f"### Step {summary.step_number}\n"
            f"**Rule:**\n{summary.rule_content}\n\n"
            f"**Code functions:** {funcs}"
        )
    return "\n\n---\n\n".join(parts)


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _split_identifier_tokens(value: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", (value or "").replace("_", " ").lower())


def _is_acknowledgement_message(user_message: str) -> bool:
    normalized = _normalize_text(user_message).lower()
    if not normalized or len(normalized) > 80:
        return False
    if any(keyword in normalized for keyword in CHANGE_KEYWORDS):
        return False
    if _is_confirmation_message(user_message):
        return False
    return any(pattern in normalized for pattern in ACKNOWLEDGEMENT_PATTERNS)


def _is_confirmation_message(user_message: str) -> bool:
    """Detect affirmative replies that should trigger apply_confirmed_changes."""
    normalized = _normalize_text(user_message).lower()
    if not normalized or len(normalized) > 100:
        return False
    if any(keyword in normalized for keyword in CHANGE_KEYWORDS if keyword not in ("change", "apply")):
        return False
    return any(pattern in normalized for pattern in CONFIRMATION_PATTERNS)


def _looks_like_information_request(user_message: str) -> bool:
    normalized = _normalize_text(user_message).lower()
    if not normalized:
        return False
    if any(normalized.startswith(prefix) for prefix in INFO_REQUEST_PREFIXES):
        return not bool(CHANGE_REQUEST_PATTERN.search(normalized))
    if normalized.endswith("?") and not CHANGE_REQUEST_PATTERN.search(normalized):
        return True
    return False


def _looks_like_change_request(user_message: str) -> bool:
    normalized = _normalize_text(user_message).lower()
    if not normalized or _is_acknowledgement_message(normalized):
        return False
    if _looks_like_information_request(normalized):
        return False
    if CHANGE_REQUEST_PATTERN.search(normalized):
        return True
    return any(phrase in normalized for phrase in ("should ", "needs to ", "need to ", "make sure "))


def _infer_primary_change_type(user_message: str) -> str:
    normalized = _normalize_text(user_message).lower()
    if any(token in normalized for token in ("code", "python", "function", "source file")):
        return "code"
    return "rule"


def _step_match_score(user_message: str, summary: StepRuleSummary) -> float:
    normalized = _normalize_text(user_message).lower()
    score = 0.0

    for phrase in STEP_HINTS.get(summary.step_number, ()):
        if phrase in normalized:
            score += max(2.0, float(len(phrase.split())))

    heading = summary.rule_content.splitlines()[0] if summary.rule_content else ""
    for token in _split_identifier_tokens(heading):
        if len(token) < 4 or token in GENERIC_MATCH_TOKENS:
            continue
        if token in normalized:
            score += 0.5

    for function_name in summary.code_functions:
        for token in _split_identifier_tokens(function_name):
            if len(token) < 4 or token in GENERIC_MATCH_TOKENS:
                continue
            if token in normalized:
                score += 0.35

    return score


def _infer_target_steps(user_message: str, summaries: list[StepRuleSummary]) -> list[int]:
    explicit_steps = sorted({int(match) for match in EXPLICIT_STEP_PATTERN.findall(user_message)})
    if explicit_steps:
        return explicit_steps

    scores = [(summary.step_number, _step_match_score(user_message, summary)) for summary in summaries]
    best_score = max((score for _, score in scores), default=0.0)
    if best_score < 2.0:
        return []

    best_steps = [step_number for step_number, score in scores if score == best_score]
    if len(best_steps) != 1:
        return []

    return best_steps


def _infer_steps_from_assistant_text(assistant_text: str) -> list[int]:
    """Extract step numbers mentioned in the assistant's response as a fallback."""
    return sorted({int(m) for m in EXPLICIT_STEP_PATTERN.findall(assistant_text)})


def _build_direct_change_decision(
    user_message: str,
    summaries: list[StepRuleSummary],
    assistant_text: str = "",
) -> ChatDecision | None:
    if not _looks_like_change_request(user_message):
        return None

    step_numbers = _infer_target_steps(user_message, summaries)
    if not step_numbers and assistant_text:
        step_numbers = _infer_steps_from_assistant_text(assistant_text)
        if step_numbers:
            logger.info(
                "step_inferred_from_assistant",
                steps=step_numbers,
                source="assistant_text",
            )
    if not step_numbers:
        return None

    raw_decision = ChatDecision(
        proposed_changes=[
            ProposedChange(
                step_number=step_number,
                change_type=_infer_primary_change_type(user_message),
                description=_normalize_text(user_message),
            )
            for step_number in step_numbers
        ]
    )
    return _finalize_decision(raw_decision, summaries, user_message)


def _format_step_labels(step_numbers: list[int]) -> str:
    labels = [f"Step {step_number} ({STEP_TITLES.get(step_number, f'Step {step_number}')})" for step_number in step_numbers]
    if not labels:
        return "the pipeline"
    if len(labels) == 1:
        return labels[0]
    return ", ".join(labels[:-1]) + f" and {labels[-1]}"


def _build_direct_change_response(user_message: str, decision: ChatDecision) -> str:
    step_numbers = sorted({change.step_number for change in decision.proposed_changes})
    includes_rule_change = any(change.change_type == "rule" for change in decision.proposed_changes)
    change_scope = "the rule update and matching logic changes" if includes_rule_change else "the logic update"
    return (
        f"I understood this as a direct change request for {_format_step_labels(step_numbers)}. "
        f"I've prepared {change_scope} for your approval: {_normalize_text(user_message)}"
    )


def _compress_question_response(
    assistant_text: str,
    decision: ChatDecision,
) -> tuple[str, ChatDecision]:
    if decision.proposed_changes or not decision.questions:
        return assistant_text, decision

    first_question = _normalize_text(decision.questions[0])
    if not first_question:
        return assistant_text, ChatDecision()

    compressed = ChatDecision(questions=[first_question], needs_confirmation=False)
    return f"I need one detail before I apply this: {first_question}", compressed


def _parse_metadata(raw: str | None) -> dict:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        return {}


def _proposal_fingerprint(change: ProposedChange | dict) -> tuple[int, str, str]:
    if isinstance(change, ProposedChange):
        step_number = change.step_number
        change_type = change.change_type
        description = change.description
    else:
        step_number = int(change.get("step_number", 0))
        change_type = str(change.get("change_type", "")).strip().lower()
        description = str(change.get("description", ""))
    return (
        step_number,
        change_type,
        _normalize_text(description).lower(),
    )


def _strip_fencing(content: str) -> str:
    stripped = content.strip()
    if stripped.startswith("```python"):
        stripped = stripped[len("```python"):].strip()
    elif stripped.startswith("```json"):
        stripped = stripped[len("```json"):].strip()
    elif stripped.startswith("```"):
        stripped = stripped[3:].strip()
    if stripped.endswith("```"):
        stripped = stripped[:-3].strip()
    return stripped


async def _persist_message(
    session: AsyncSession,
    session_id: str,
    role: str,
    content: str,
    metadata: dict | None = None,
) -> ChatMessage:
    message = ChatMessage(
        session_id=session_id,
        role=role,
        content=content,
        metadata_json=json.dumps(metadata or {}),
    )
    session.add(message)
    await session.commit()
    await session.refresh(message)
    return message


async def _load_chat_context(
    session: AsyncSession,
    session_id: str,
) -> tuple[list[ChatMessage], list[StepRuleSummary], str, list[Message]]:
    result = await session.execute(
        select(ChatMessage)
        .where(ChatMessage.session_id == session_id)
        .order_by(ChatMessage.created_at.desc())
        .limit(30)
    )
    history_rows = list(reversed(result.scalars().all()))
    summaries = await load_step_context(session)
    context_block = _build_context_block(summaries)

    llm_messages = [
        Message(role="user", content=f"Current pipeline rules and code:\n\n{context_block}"),
        Message(role="assistant", content="Context loaded. I am ready to help with pipeline changes."),
    ]
    for row in history_rows:
        llm_messages.append(Message(role=row.role, content=row.content))

    return history_rows, summaries, context_block, llm_messages


def _build_history_transcript(history_rows: list[ChatMessage]) -> str:
    lines: list[str] = []
    for row in history_rows:
        meta = _parse_metadata(row.metadata_json)
        suffix = ""
        if meta.get("proposal_status") == "applied":
            suffix = " [proposal applied]"
        elif meta.get("proposal_status") == "pending":
            suffix = " [proposal pending]"
        lines.append(f"{row.role.upper()}{suffix}: {row.content}")
    return "\n".join(lines)


def _finalize_decision(
    decision: ChatDecision,
    summaries: list[StepRuleSummary],
    user_message: str,
) -> ChatDecision:
    if _is_acknowledgement_message(user_message):
        return ChatDecision()

    native_steps = {summary.step_number for summary in summaries if summary.code_functions}
    normalized_questions = [_normalize_text(question) for question in decision.questions if _normalize_text(question)]

    seen: set[tuple[int, str, str]] = set()
    proposed_changes: list[ProposedChange] = []
    rule_descriptions_by_step: dict[int, list[str]] = defaultdict(list)
    has_code_change_by_step: dict[int, bool] = defaultdict(bool)

    for change in decision.proposed_changes:
        change_type = change.change_type.strip().lower()
        if change_type not in {"rule", "code"}:
            continue
        if not (1 <= change.step_number <= STEP_COUNT):
            continue
        normalized = ProposedChange(
            step_number=change.step_number,
            change_type=change_type,
            description=_normalize_text(change.description),
        )
        if not normalized.description:
            continue
        fingerprint = _proposal_fingerprint(normalized)
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        proposed_changes.append(normalized)
        if normalized.change_type == "rule":
            rule_descriptions_by_step[normalized.step_number].append(normalized.description)
        else:
            has_code_change_by_step[normalized.step_number] = True

    for step_number, descriptions in rule_descriptions_by_step.items():
        if step_number not in native_steps or has_code_change_by_step[step_number]:
            continue
        synthetic = ProposedChange(
            step_number=step_number,
            change_type="code",
            description="Sync native step logic with the updated rules: " + "; ".join(descriptions),
        )
        fingerprint = _proposal_fingerprint(synthetic)
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        proposed_changes.append(synthetic)

    needs_confirmation = bool(proposed_changes) and not normalized_questions
    return ChatDecision(
        proposed_changes=proposed_changes,
        needs_confirmation=needs_confirmation,
        questions=normalized_questions,
    )


async def _generate_assistant_text(
    llm: LLMProvider,
    llm_messages: list[Message],
) -> str:
    response = await llm.complete(
        system_prompt=CHAT_RESPONSE_SYSTEM_PROMPT,
        messages=llm_messages,
        temperature=0.2,
        max_tokens=4096,
    )
    return response.content.strip() or "I need a bit more detail about the change you want."


async def _generate_chat_decision(
    llm: LLMProvider,
    context_block: str,
    history_rows: list[ChatMessage],
    assistant_text: str,
    user_message: str,
    summaries: list[StepRuleSummary],
) -> ChatDecision:
    if _is_acknowledgement_message(user_message):
        return ChatDecision()

    transcript = _build_history_transcript(history_rows)
    decision = await llm.complete_structured(
        system_prompt=CHAT_DECISION_SYSTEM_PROMPT,
        messages=[
            Message(
                role="user",
                content=(
                    "Current pipeline state:\n\n"
                    f"{context_block}\n\n"
                    "Recent conversation:\n"
                    f"{transcript}\n\n"
                    "Latest assistant reply:\n"
                    f"{assistant_text}\n\n"
                    "Latest user message:\n"
                    f"{user_message}\n\n"
                    "Return structured metadata for the latest user turn only."
                ),
            )
        ],
        response_model=ChatDecision,
        temperature=0.0,
    )
    logger.info(
        "llm_raw_decision",
        proposed_changes=len(decision.proposed_changes),
        raw_changes=[
            {"step": c.step_number, "type": c.change_type, "desc": c.description[:80]}
            for c in decision.proposed_changes
        ],
        questions=decision.questions,
    )
    finalized = _finalize_decision(decision, summaries, user_message)
    return _compress_question_response(assistant_text, finalized)[1]


async def _persist_assistant_response(
    session: AsyncSession,
    session_id: str,
    assistant_text: str,
    decision: ChatDecision,
) -> ChatResponse:
    metadata: dict[str, object] = {}
    if decision.proposed_changes:
        metadata["proposed_changes"] = [change.model_dump() for change in decision.proposed_changes]
        metadata["proposal_status"] = "pending"
    if decision.questions:
        metadata["questions"] = decision.questions

    await _persist_message(
        session,
        session_id,
        "assistant",
        assistant_text,
        metadata=metadata,
    )

    return ChatResponse(
        session_id=session_id,
        message=assistant_text,
        proposed_changes=decision.proposed_changes,
        needs_confirmation=decision.needs_confirmation,
        questions=decision.questions,
        applied_proposals=[],
    )


def _sse_event(event: str, payload: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(payload)}\n\n"


async def handle_chat_message(
    session: AsyncSession,
    llm: LLMProvider,
    session_id: str,
    user_message: str,
) -> ChatResponse:
    await _persist_message(session, session_id, "user", user_message)

    if _is_acknowledgement_message(user_message):
        return await _persist_assistant_response(
            session,
            session_id,
            "You're welcome.",
            ChatDecision(),
        )

    # Auto-apply pending proposals when user confirms with "yes", "go ahead", etc.
    if _is_confirmation_message(user_message):
        _, pending = await _get_latest_pending_proposal_message(session, session_id)
        if pending:
            logger.info("auto_confirm_triggered", session_id=session_id, user_message=user_message)
            return await apply_confirmed_changes(session, llm, session_id)

    history_rows, summaries, context_block, llm_messages = await _load_chat_context(session, session_id)
    assistant_text = await _generate_assistant_text(llm, llm_messages)
    decision = await _generate_chat_decision(
        llm,
        context_block,
        history_rows,
        assistant_text,
        user_message,
        summaries,
    )
    if not decision.proposed_changes:
        direct_decision = _build_direct_change_decision(user_message, summaries, assistant_text)
        if direct_decision:
            if not decision.questions:
                assistant_text = _build_direct_change_response(user_message, direct_decision)
            decision = direct_decision
    logger.info(
        "chat_decision_result",
        session_id=session_id,
        proposed_changes=len(decision.proposed_changes),
        needs_confirmation=decision.needs_confirmation,
        questions=len(decision.questions),
    )
    assistant_text, decision = _compress_question_response(assistant_text, decision)
    return await _persist_assistant_response(session, session_id, assistant_text, decision)


async def stream_chat_message(
    session: AsyncSession,
    llm: LLMProvider,
    session_id: str,
    user_message: str,
) -> AsyncIterator[str]:
    await _persist_message(session, session_id, "user", user_message)
    yield _sse_event("start", {"session_id": session_id})

    if _is_acknowledgement_message(user_message):
        assistant_text = "You're welcome."
        yield _sse_event("delta", {"text": assistant_text})
        response = await _persist_assistant_response(
            session,
            session_id,
            assistant_text,
            ChatDecision(),
        )
        yield _sse_event("result", response.model_dump())
        return

    # Auto-apply pending proposals when user confirms with "yes", "go ahead", etc.
    if _is_confirmation_message(user_message):
        _, pending = await _get_latest_pending_proposal_message(session, session_id)
        if pending:
            logger.info("auto_confirm_triggered", session_id=session_id, user_message=user_message)
            yield _sse_event("thinking", {"label": "Applying confirmed changes"})
            response = await apply_confirmed_changes(session, llm, session_id)
            yield _sse_event("delta", {"text": response.message})
            yield _sse_event("result", response.model_dump())
            return

    history_rows, summaries, context_block, llm_messages = await _load_chat_context(session, session_id)
    yield _sse_event("thinking", {"label": "Analyzing your request"})

    chunks: list[str] = []
    try:
        async for chunk in llm.stream_complete(
            system_prompt=CHAT_RESPONSE_SYSTEM_PROMPT,
            messages=llm_messages,
            temperature=0.2,
            max_tokens=4096,
        ):
            if not chunk:
                continue
            chunks.append(chunk)
            yield _sse_event("delta", {"text": chunk})
    except Exception as exc:
        logger.warning("chat_stream_fallback_to_complete", error=str(exc))
        assistant_text = await _generate_assistant_text(llm, llm_messages)
        chunks = [assistant_text]
        yield _sse_event("delta", {"text": assistant_text})

    assistant_text = "".join(chunks).strip() or "I need a bit more detail about the change you want."
    yield _sse_event("thinking", {"label": "Preparing change summary"})

    decision = await _generate_chat_decision(
        llm,
        context_block,
        history_rows,
        assistant_text,
        user_message,
        summaries,
    )
    if not decision.proposed_changes:
        direct_decision = _build_direct_change_decision(user_message, summaries, assistant_text)
        if direct_decision:
            if not decision.questions:
                assistant_text = _build_direct_change_response(user_message, direct_decision)
            decision = direct_decision
    logger.info(
        "chat_decision_result",
        session_id=session_id,
        proposed_changes=len(decision.proposed_changes),
        needs_confirmation=decision.needs_confirmation,
        questions=len(decision.questions),
    )
    assistant_text, decision = _compress_question_response(assistant_text, decision)
    response = await _persist_assistant_response(session, session_id, assistant_text, decision)
    yield _sse_event("result", response.model_dump())


async def _get_latest_pending_proposal_message(
    session: AsyncSession,
    session_id: str,
) -> tuple[ChatMessage | None, list[ProposedChange]]:
    result = await session.execute(
        select(ChatMessage)
        .where(ChatMessage.session_id == session_id, ChatMessage.role == "assistant")
        .order_by(ChatMessage.created_at.desc())
        .limit(100)
    )
    for row in result.scalars().all():
        metadata = _parse_metadata(row.metadata_json)
        if metadata.get("proposal_status") != "pending":
            continue
        raw_changes = metadata.get("proposed_changes") or []
        try:
            changes = [ProposedChange(**change) for change in raw_changes]
        except (TypeError, ValueError):
            continue
        if changes:
            return row, changes
    return None, []


async def _apply_rule_change(
    session: AsyncSession,
    llm: LLMProvider,
    step_number: int,
    current_rule_content: str,
    description: str,
) -> tuple[str, int]:
    response = await llm.complete(
        system_prompt=APPLY_RULE_CHANGE_PROMPT,
        messages=[
            Message(
                role="user",
                content=(
                    f"Current rule for step {step_number}:\n"
                    f"{current_rule_content}\n\n"
                    f"Requested change: {description}"
                ),
            )
        ],
        temperature=0.0,
        max_tokens=4096,
    )
    updated_content = _strip_fencing(response.content)
    new_rule = await create_rule_version(session, step_number, updated_content, created_by="chat")
    return updated_content, new_rule.id


async def _apply_code_change(
    session: AsyncSession,
    llm: LLMProvider,
    step_number: int,
    current_code_content: str,
    active_rule_content: str,
    description: str,
    parent_version_id: int | None,
) -> tuple[str, int]:
    response = await llm.complete(
        system_prompt=APPLY_CODE_CHANGE_PROMPT,
        messages=[
            Message(
                role="user",
                content=(
                    f"Active rules for step {step_number}:\n"
                    f"{active_rule_content}\n\n"
                    f"Current code for step {step_number}:\n"
                    f"```python\n{current_code_content}\n```\n\n"
                    f"Requested change: {description}"
                ),
            )
        ],
        temperature=0.0,
        max_tokens=8192,
    )
    updated_content = _strip_fencing(response.content)
    new_code = await create_code_version(
        session,
        step_number=step_number,
        content=updated_content,
        parent_version_id=parent_version_id,
        changed_function=None,
        created_by="chat",
    )
    return updated_content, new_code.id


def _is_restore_request(description: str) -> bool:
    text = description.strip().lower()
    return any(keyword in text for keyword in RESTORE_KEYWORDS)


async def _get_previous_rule_version(
    session: AsyncSession,
    step_number: int,
) -> RuleVersion | None:
    result = await session.execute(
        select(RuleVersion)
        .where(RuleVersion.step_number == step_number)
        .order_by(RuleVersion.version.desc())
        .limit(2)
    )
    versions = result.scalars().all()
    if len(versions) < 2:
        return None
    return versions[1]


async def _get_previous_code_version(
    session: AsyncSession,
    step_number: int,
) -> CodeVersion | None:
    result = await session.execute(
        select(CodeVersion)
        .where(CodeVersion.step_number == step_number)
        .order_by(CodeVersion.version.desc())
        .limit(2)
    )
    versions = result.scalars().all()
    if len(versions) < 2:
        return None
    return versions[1]


async def _restore_previous_rule_version(
    session: AsyncSession,
    step_number: int,
) -> tuple[str, int] | None:
    previous_rule = await _get_previous_rule_version(session, step_number)
    if previous_rule is None:
        return None
    restored_rule = await create_rule_version(
        session,
        step_number,
        previous_rule.content,
        created_by="chat",
    )
    return restored_rule.content, restored_rule.id


async def _restore_previous_code_version(
    session: AsyncSession,
    step_number: int,
    parent_version_id: int | None,
) -> tuple[str, int] | None:
    previous_code = await _get_previous_code_version(session, step_number)
    if previous_code is None:
        return None
    restored_code = await create_code_version(
        session,
        step_number=step_number,
        content=previous_code.content,
        parent_version_id=parent_version_id,
        changed_function=None,
        created_by="chat",
    )
    return restored_code.content, restored_code.id


async def _resolve_chat_checkpoint(
    session: AsyncSession,
    job_id: str | None,
) -> JobCheckpoint | None:
    stmt = (
        select(JobCheckpoint)
        .join(JobStatus, JobStatus.job_id == JobCheckpoint.job_id)
        .where(
            JobCheckpoint.status == "AWAITING_FEEDBACK",
            JobStatus.state == "awaiting_feedback",
        )
    )
    if job_id:
        stmt = stmt.where(JobCheckpoint.job_id == job_id)
    else:
        # Only match the most recent awaiting-feedback job. Order by the
        # checkpoint's updated_at so we don't accidentally patch an older
        # abandoned job that was never completed or cancelled.
        stmt = stmt.order_by(JobCheckpoint.updated_at.desc()).limit(1)
        logger.warning("resolve_checkpoint_no_job_id", hint="Consider passing job_id explicitly")

    result = await session.execute(stmt)
    checkpoint = result.scalar_one_or_none()
    if checkpoint is None or checkpoint.status != "AWAITING_FEEDBACK":
        return None
    return checkpoint


async def _attach_changes_to_active_checkpoint(
    session: AsyncSession,
    job_id: str | None,
    rule_version_ids: dict[int, int],
    code_version_ids: dict[int, int],
    changed_steps: set[int],
) -> bool:
    if not changed_steps:
        return False

    checkpoint = await _resolve_chat_checkpoint(session, job_id)
    if checkpoint is None:
        return False

    rule_snapshot_ids = json.loads(checkpoint.rule_snapshot_ids_json or "{}")
    for step_number, version_id in rule_version_ids.items():
        rule_snapshot_ids[str(step_number)] = version_id

    storage_keys = json.loads(checkpoint.storage_keys_json or "{}")
    code_snapshot_ids = storage_keys.get("_code_snapshot_ids")
    if not isinstance(code_snapshot_ids, dict):
        code_snapshot_ids = {}
    for step_number, version_id in code_version_ids.items():
        code_snapshot_ids[str(step_number)] = version_id
    storage_keys["_code_snapshot_ids"] = code_snapshot_ids

    failed_row_indices = json.loads(checkpoint.failed_row_indices_json or "{}")
    for step_number in changed_steps:
        failed_row_indices[str(step_number)] = [-1]

    checkpoint.rule_snapshot_ids_json = json.dumps(rule_snapshot_ids)
    checkpoint.storage_keys_json = json.dumps(storage_keys)
    checkpoint.failed_row_indices_json = json.dumps(failed_row_indices)
    await session.commit()
    return True


async def apply_confirmed_changes(
    session: AsyncSession,
    llm: LLMProvider,
    session_id: str,
    job_id: str | None = None,
) -> ChatResponse:
    proposal_row, proposed_changes = await _get_latest_pending_proposal_message(session, session_id)
    if proposal_row is None or not proposed_changes:
        message = "No pending changes to apply. Please describe what you'd like to change first."
        await _persist_message(session, session_id, "assistant", message)
        return ChatResponse(session_id=session_id, message=message)

    applied: list[str] = []
    errors: list[str] = []
    applied_rule_version_ids: dict[int, int] = {}
    applied_code_version_ids: dict[int, int] = {}
    changed_steps: set[int] = set()

    changes_by_step: dict[int, dict[str, list[ProposedChange]]] = {}
    step_order: list[int] = []
    for change in proposed_changes:
        if change.step_number not in changes_by_step:
            changes_by_step[change.step_number] = {"rule": [], "code": []}
            step_order.append(change.step_number)
        changes_by_step[change.step_number][change.change_type].append(change)

    for step_number in step_order:
        grouped = changes_by_step[step_number]
        rule_version = await get_latest_rule(session, step_number)
        code_version = await get_latest_code(session, step_number)

        current_rule_content = rule_version.content if rule_version else ""
        current_code_content = code_version.content if code_version else ""
        current_code_parent_id = code_version.id if code_version else None
        all_step_changes = grouped["rule"] + grouped["code"]
        all_restore_requests = bool(all_step_changes) and all(
            _is_restore_request(change.description) for change in all_step_changes
        )

        if all_restore_requests:
            if grouped["rule"]:
                if not rule_version:
                    errors.append(f"Step {step_number}: no rule found")
                else:
                    restored_rule = await _restore_previous_rule_version(session, step_number)
                    if restored_rule is None:
                        errors.append(f"Step {step_number} rule: no previous rule version to restore")
                    else:
                        current_rule_content, new_rule_version_id = restored_rule
                        applied_rule_version_ids[step_number] = new_rule_version_id
                        changed_steps.add(step_number)
                        applied.append(f"Step {step_number} rule restored to previous version")

            if (grouped["code"] or grouped["rule"]) and code_version:
                restored_code = await _restore_previous_code_version(
                    session,
                    step_number,
                    current_code_parent_id,
                )
                if restored_code is None:
                    errors.append(f"Step {step_number} code: no previous code version to restore")
                else:
                    current_code_content, new_code_version_id = restored_code
                    current_code_parent_id = new_code_version_id
                    applied_code_version_ids[step_number] = new_code_version_id
                    changed_steps.add(step_number)
                    applied.append(f"Step {step_number} code restored to previous version")
            elif grouped["code"]:
                errors.append(f"Step {step_number}: no code found for restore request")
            continue

        for change in grouped["rule"]:
            if not rule_version and not current_rule_content:
                errors.append(f"Step {step_number}: no rule found")
                continue
            try:
                current_rule_content, new_rule_version_id = await _apply_rule_change(
                    session,
                    llm,
                    step_number,
                    current_rule_content,
                    change.description,
                )
                applied_rule_version_ids[step_number] = new_rule_version_id
                changed_steps.add(step_number)
                applied.append(f"Step {step_number} rule updated: {change.description}")
            except Exception as exc:
                logger.error("chat_apply_rule_error", step=step_number, error=str(exc))
                errors.append(f"Step {step_number} rule: {exc}")

        explicit_code_changes = grouped["code"]
        if explicit_code_changes:
            if not code_version and not current_code_content:
                for change in explicit_code_changes:
                    errors.append(f"Step {step_number}: no code found for '{change.description}'")
            else:
                for change in explicit_code_changes:
                    try:
                        current_code_content, new_code_version_id = await _apply_code_change(
                            session,
                            llm,
                            step_number,
                            current_code_content,
                            current_rule_content,
                            change.description,
                            current_code_parent_id,
                        )
                        current_code_parent_id = new_code_version_id
                        applied_code_version_ids[step_number] = new_code_version_id
                        changed_steps.add(step_number)
                        applied.append(f"Step {step_number} code updated: {change.description}")
                    except Exception as exc:
                        logger.error("chat_apply_code_error", step=step_number, error=str(exc))
                        errors.append(f"Step {step_number} code: {exc}")
        elif grouped["rule"] and code_version:
            try:
                sync_description = (
                    "Synchronize the native step logic with the newly updated rules. "
                    "User-requested rule changes: "
                    + "; ".join(change.description for change in grouped["rule"])
                )
                current_code_content, new_code_version_id = await _apply_code_change(
                    session,
                    llm,
                    step_number,
                    current_code_content,
                    current_rule_content,
                    sync_description,
                    current_code_parent_id,
                )
                current_code_parent_id = new_code_version_id
                applied_code_version_ids[step_number] = new_code_version_id
                changed_steps.add(step_number)
                applied.append(f"Step {step_number} code synced to updated rules")
            except Exception as exc:
                logger.error("chat_apply_code_sync_error", step=step_number, error=str(exc))
                errors.append(f"Step {step_number} code sync: {exc}")

    checkpoint_synced = await _attach_changes_to_active_checkpoint(
        session,
        job_id,
        applied_rule_version_ids,
        applied_code_version_ids,
        changed_steps,
    )

    metadata = _parse_metadata(proposal_row.metadata_json)
    metadata["proposal_status"] = "applied" if not errors else "consumed"
    metadata["applied_at"] = datetime.now(timezone.utc).isoformat()
    metadata["applied_proposals"] = [change.model_dump() for change in proposed_changes]
    proposal_row.metadata_json = json.dumps(metadata)
    await session.commit()

    parts: list[str] = []
    if applied:
        parts.append("Applied changes:\n" + "\n".join(f"- {entry}" for entry in applied))
    if checkpoint_synced:
        parts.append("The active paused job will use these changes the next time processing resumes.")
    elif applied and not errors:
        parts.append("Upload new files to process them with the updated rules.")
    if errors:
        parts.append("Errors:\n" + "\n".join(f"- {entry}" for entry in errors))
    if not parts:
        parts.append("No changes were applied.")
    message = "\n\n".join(parts)

    await _persist_message(session, session_id, "user", "confirmed")
    await _persist_message(
        session,
        session_id,
        "assistant",
        message,
        metadata={
            "applied": applied,
            "errors": errors,
            "applied_proposals": [change.model_dump() for change in proposed_changes],
        },
    )

    return ChatResponse(
        session_id=session_id,
        message=message,
        proposed_changes=[],
        needs_confirmation=False,
        questions=[],
        applied_proposals=proposed_changes,
    )
