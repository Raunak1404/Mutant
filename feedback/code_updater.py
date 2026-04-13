"""
feedback/code_updater.py — AST-based function-level code updates

When the review agent detects failures in a native step, this module:
1. Identifies which function is likely broken from the failure patterns
2. Extracts just that function (~20-80 lines) from the full source
3. Sends it to the LLM for regeneration
4. Uses AST-based replacement to swap the function back in
5. Creates a new CodeVersion in the DB
"""

from __future__ import annotations

import ast
import json
import textwrap

from sqlalchemy.ext.asyncio import AsyncSession

from db.code_versions import create_code_version, get_code_by_version_id
from db.rules import get_rule_by_version_id
from llm.provider import LLMProvider
from models.messages import Message
from models.results import FeedbackQuestion, UserFeedback
from utils.logging import get_logger

logger = get_logger(__name__)

CODE_UPDATE_SYSTEM_PROMPT = """You are a Python code editor for a data transformation system.
You will be given:
1. A single Python function that needs to be fixed
2. The full file context (for reference only — do NOT return the full file)
3. User feedback describing the failure

Return ONLY the corrected function definition (starting with 'def ...').
Do NOT include any explanation, markdown fencing, or other text — just the Python function code.
Preserve the function signature, docstring style, and coding conventions.

IMPORTANT: If the function references module-level constants (e.g., SAP_COLUMNS, RENAME_MAP),
those constants are defined elsewhere in the file and cannot be changed here.
Your fix MUST work with those constants as-is — adapt the function logic instead."""

CONSTANT_UPDATE_SYSTEM_PROMPT = """You are a Python code editor for a data transformation system.
You will be given:
1. A module-level constant/variable assignment that needs to be fixed
2. The full file context (for reference only — do NOT return the full file)
3. User feedback describing the failure

Return ONLY the corrected assignment statement (e.g., MY_CONSTANT = [...]).
Do NOT include any explanation, markdown fencing, or other text — just the Python assignment.
Keep the same variable name and follow existing coding conventions.
When fixing collections (dicts, sets, lists), prefer adding entries over replacing existing ones."""

IDENTIFY_TARGET_PROMPT = """Given these failure patterns from a data processing step, identify which target (function or module-level constant) is most likely responsible for the failure.

Available functions in the step code:
{function_list}

Module-level constants/variables:
{constant_list}

Step rules (current requirements the code must satisfy):
{rule_content}

Failure patterns:
{failure_patterns}

User feedback:
{user_feedback}

GUIDELINES for choosing the correct target:
1. Read the code carefully — understand how constants relate to functions before choosing.
2. Target a constant when its value is directly causing the failure.
3. Target a function when the processing logic itself is wrong.
4. Consider the step rules when deciding — the fix must satisfy all stated requirements.

Return a JSON object:
- If a function is broken: {{"target_type": "function", "target_name": "func_name", "reason": "why"}}
- If a module-level constant is wrong (simple permanent fix only): {{"target_type": "constant", "target_name": "CONST_NAME", "reason": "why"}}

Return ONLY the JSON object."""


def extract_function(source: str, func_name: str) -> str | None:
    """Extract a single function's source code from a Python file using AST.

    Args:
        source: Full Python source code
        func_name: Name of the function to extract

    Returns:
        The function source code, or None if not found
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        logger.error("ast_parse_error", func_name=func_name)
        return None

    lines = source.splitlines(keepends=True)

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == func_name:
            # Get the start line (1-indexed in AST)
            start_line = node.lineno - 1  # Convert to 0-indexed

            # Find the end line — look for the next node at the same or lower indentation
            end_line = node.end_lineno  # end_lineno is 1-indexed, already points past last line

            if end_line is None:
                # Fallback: scan for dedent
                end_line = _find_function_end(lines, start_line)

            func_source = "".join(lines[start_line:end_line])
            return func_source

    return None


def _find_function_end(lines: list[str], start_line: int) -> int:
    """Fallback: find function end by indentation analysis."""
    if start_line >= len(lines):
        return len(lines)

    # Get the indentation of the def line
    def_line = lines[start_line]
    def_indent = len(def_line) - len(def_line.lstrip())

    # Scan forward for the first non-empty line at same or lower indentation
    in_body = False
    for i in range(start_line + 1, len(lines)):
        line = lines[i]
        stripped = line.strip()

        if not stripped:
            continue  # Skip blank lines

        line_indent = len(line) - len(line.lstrip())

        if not in_body and line_indent > def_indent:
            in_body = True

        if in_body and line_indent <= def_indent:
            return i

    return len(lines)


def replace_function(source: str, func_name: str, new_func_source: str) -> str:
    """Replace a function in the source code using AST-based location.

    Args:
        source: Full Python source code
        func_name: Name of the function to replace
        new_func_source: New function source code (complete def block)

    Returns:
        Updated full source code
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        logger.error("ast_parse_error_replace", func_name=func_name)
        return source

    lines = source.splitlines(keepends=True)

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == func_name:
            start_line = node.lineno - 1
            end_line = node.end_lineno if node.end_lineno else _find_function_end(lines, start_line)

            # Get the indentation of the original function
            original_indent = len(lines[start_line]) - len(lines[start_line].lstrip())

            # Normalize tabs to 4 spaces before re-indentation to prevent
            # TabError when mixing tabs and spaces from LLM-generated code.
            new_func_source = new_func_source.expandtabs(4)
            # Ensure new function has correct indentation
            new_lines = new_func_source.splitlines(keepends=True)
            if new_lines:
                # Detect indentation of new code
                first_non_empty = next((l for l in new_lines if l.strip()), new_lines[0])
                new_indent = len(first_non_empty) - len(first_non_empty.lstrip())

                # Re-indent if needed
                if new_indent != original_indent:
                    indent_diff = original_indent - new_indent
                    adjusted_lines = []
                    for line in new_lines:
                        if line.strip():  # Non-empty line
                            current_indent = len(line) - len(line.lstrip())
                            new_line_indent = max(0, current_indent + indent_diff)
                            adjusted_lines.append(" " * new_line_indent + line.lstrip())
                        else:
                            adjusted_lines.append(line)
                    new_lines = adjusted_lines

                # Ensure trailing newline
                if new_lines and not new_lines[-1].endswith("\n"):
                    new_lines[-1] += "\n"

            # Replace the lines
            result_lines = lines[:start_line] + new_lines + lines[end_line:]
            return "".join(result_lines)

    logger.warning("function_not_found_for_replace", func_name=func_name)
    return source


def list_functions(source: str) -> list[dict[str, str]]:
    """List all top-level functions in a Python source file.

    Returns list of {"name": str, "line": int, "docstring": str | None}
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []

    functions = []
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.FunctionDef):
            docstring = ast.get_docstring(node)
            functions.append({
                "name": node.name,
                "line": node.lineno,
                "docstring": docstring[:100] if docstring else None,
            })
    return functions


def list_module_constants(source: str) -> list[dict[str, str]]:
    """List module-level assignments (constants) in a Python source file.

    Returns list of {"name": str, "line": int, "preview": str}
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []

    lines = source.splitlines()
    constants = []
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    start = node.lineno - 1
                    end = node.end_lineno or node.lineno
                    value_preview = "\n".join(lines[start:end])
                    constants.append({
                        "name": target.id,
                        "line": node.lineno,
                        "preview": value_preview[:200],
                    })
    return constants


def extract_constant(source: str, const_name: str) -> str | None:
    """Extract a module-level constant assignment from source using AST.

    Args:
        source: Full Python source code
        const_name: Name of the constant to extract

    Returns:
        The constant assignment source code, or None if not found
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        logger.error("ast_parse_error", const_name=const_name)
        return None

    lines = source.splitlines(keepends=True)

    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == const_name:
                    start = node.lineno - 1
                    end = node.end_lineno or node.lineno
                    return "".join(lines[start:end])
    return None


def replace_constant(source: str, const_name: str, new_const_source: str) -> str:
    """Replace a module-level constant in the source code using AST-based location.

    Args:
        source: Full Python source code
        const_name: Name of the constant to replace
        new_const_source: New constant assignment source code

    Returns:
        Updated full source code
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        logger.error("ast_parse_error_replace_constant", const_name=const_name)
        return source

    lines = source.splitlines(keepends=True)

    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == const_name:
                    start = node.lineno - 1
                    end = node.end_lineno or node.lineno

                    new_lines = new_const_source.splitlines(keepends=True)
                    if new_lines and not new_lines[-1].endswith("\n"):
                        new_lines[-1] += "\n"

                    result_lines = lines[:start] + new_lines + lines[end:]
                    return "".join(result_lines)

    logger.warning("constant_not_found_for_replace", const_name=const_name)
    return source


async def identify_broken_function(
    llm: LLMProvider,
    code_content: str,
    failure_patterns: list[str],
    user_feedback: str,
    rule_content: str = "",
) -> dict[str, str] | None:
    """Use LLM to identify which target (function or constant) is most likely broken.

    Returns dict with 'target_type' ('function' or 'constant') and 'target_name',
    or None if identification failed.
    """
    functions = list_functions(code_content)
    constants = list_module_constants(code_content)

    if not functions and not constants:
        return None

    function_list = "\n".join(
        f"- {f['name']} (line {f['line']}): {f['docstring'] or 'no docstring'}"
        for f in functions
    ) or "(none)"

    constant_list = "\n".join(
        f"- {c['name']} (line {c['line']}): {c['preview']}"
        for c in constants
    ) or "(none)"

    try:
        response = await llm.complete(
            system_prompt=IDENTIFY_TARGET_PROMPT.format(
                function_list=function_list,
                constant_list=constant_list,
                rule_content=rule_content[:3000] or "(no rules available)",
                failure_patterns=json.dumps(failure_patterns),
                user_feedback=user_feedback,
            ),
            messages=[Message(role="user", content="Identify the broken target.")],
            temperature=0.0,
            max_tokens=256,
        )
        content = response.content.strip()
        if "```json" in content:
            content = content.split("```json", 1)[1].split("```", 1)[0]
        elif "```" in content:
            content = content.split("```", 1)[1].split("```", 1)[0]

        data = json.loads(content.strip())

        # Support new format with target_type/target_name
        target_type = data.get("target_type", "function")
        target_name = data.get("target_name") or data.get("function_name")

        if not target_name:
            return None

        # Verify the target exists
        if target_type == "function" and any(f["name"] == target_name for f in functions):
            return {"target_type": "function", "target_name": target_name}
        if target_type == "constant" and any(c["name"] == target_name for c in constants):
            return {"target_type": "constant", "target_name": target_name}

        # Fallback: try as function name for backward compatibility
        if any(f["name"] == target_name for f in functions):
            return {"target_type": "function", "target_name": target_name}
        if any(c["name"] == target_name for c in constants):
            return {"target_type": "constant", "target_name": target_name}

        logger.warning("identified_target_not_found", target_type=target_type, target_name=target_name)
        return None

    except Exception as exc:
        logger.error("identify_target_error", error=str(exc))
        return None


async def update_code_from_feedback(
    session: AsyncSession,
    llm: LLMProvider,
    job_id: str,
    step_code_snapshot_ids: dict[int, int],
    questions: list[FeedbackQuestion],
    answers: list[UserFeedback],
    rule_snapshot_ids: dict[int, int] | None = None,
) -> dict[int, int]:
    """
    Update code versions based on user feedback using AST-based function replacement.
    Returns {step_number: new_code_version_id}.
    """
    answer_map = {fb.question_id: fb.answer for fb in answers}

    # Group questions by step
    step_questions: dict[int, list[tuple[FeedbackQuestion, str]]] = {}
    for q in questions:
        answer = answer_map.get(q.question_id, "")
        if not answer or not answer.strip():
            continue
        step_questions.setdefault(q.step_number, []).append((q, answer))

    new_version_ids: dict[int, int] = {}

    for step_number, qa_pairs in step_questions.items():
        version_id = step_code_snapshot_ids.get(step_number)
        if not version_id:
            continue

        code_version = await get_code_by_version_id(session, version_id)
        if not code_version:
            logger.warning("code_version_not_found", version_id=version_id)
            continue

        # Fetch rule content for this step (gives the AI full context)
        rule_content = ""
        if rule_snapshot_ids:
            rule_version_id = rule_snapshot_ids.get(step_number)
            if rule_version_id:
                rule_version = await get_rule_by_version_id(session, rule_version_id)
                if rule_version:
                    rule_content = rule_version.content

        # Collect failure patterns and feedback
        failure_patterns = [q.failure_pattern for q, _ in qa_pairs if q.failure_pattern]
        user_feedback_text = "\n".join(
            f"Q: {q.question_text}\nA: {answer}\nPattern: {q.failure_pattern}"
            for q, answer in qa_pairs
        )

        # Step 1: Identify which target is broken (function or constant)
        target = await identify_broken_function(
            llm, code_version.content, failure_patterns, user_feedback_text,
            rule_content=rule_content,
        )

        if not target:
            logger.warning("could_not_identify_target", step=step_number)
            continue

        target_type = target["target_type"]
        target_name = target["target_name"]

        # Step 2: Extract the target
        if target_type == "function":
            target_source = extract_function(code_version.content, target_name)
            system_prompt = CODE_UPDATE_SYSTEM_PROMPT
            target_label = "function definition"
        else:
            target_source = extract_constant(code_version.content, target_name)
            system_prompt = CONSTANT_UPDATE_SYSTEM_PROMPT
            target_label = "constant assignment"

        if not target_source:
            logger.warning("could_not_extract_target", target_type=target_type, target_name=target_name)
            continue

        # Step 3: Send target to LLM for fix
        rules_section = ""
        if rule_content:
            rules_section = f"""
Step rules (requirements the code must satisfy):
{rule_content[:3000]}
"""

        user_msg = f"""{'Function' if target_type == 'function' else 'Constant'} to fix (from step {step_number} code):
```python
{target_source}
```

Full file context (for reference):
```python
{code_version.content}
```
{rules_section}
User feedback:
{user_feedback_text}

Return ONLY the corrected {target_label}."""

        try:
            response = await llm.complete(
                system_prompt=system_prompt,
                messages=[Message(role="user", content=user_msg)],
                temperature=0.0,
                max_tokens=4096,
            )
            new_target = response.content.strip()

            # Strip markdown fencing if present
            if new_target.startswith("```python"):
                new_target = new_target[len("```python"):].strip()
            if new_target.startswith("```"):
                new_target = new_target[3:].strip()
            if new_target.endswith("```"):
                new_target = new_target[:-3].strip()

            # Step 4: AST-based replacement
            if target_type == "function":
                new_content = replace_function(code_version.content, target_name, new_target)
            else:
                new_content = replace_constant(code_version.content, target_name, new_target)

            # Verify the new code is valid Python
            try:
                ast.parse(new_content)
            except SyntaxError as syn_err:
                logger.error(
                    "code_update_syntax_error",
                    step=step_number,
                    target_type=target_type,
                    target_name=target_name,
                    error=str(syn_err),
                )
                continue

            # Step 5: Create new version
            new_code = await create_code_version(
                session,
                step_number=step_number,
                content=new_content,
                parent_version_id=code_version.id,
                changed_function=target_name,
                created_by=job_id,
            )
            new_version_ids[step_number] = new_code.id
            logger.info(
                "code_updated",
                job_id=job_id,
                step=step_number,
                func=target_name,
                old_version=code_version.version,
                new_version=new_code.version,
            )

        except Exception as exc:
            logger.error("code_update_error", step=step_number, target=target_name, error=str(exc))

    return new_version_ids
