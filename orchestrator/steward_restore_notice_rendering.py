"""User-facing rendering helpers for named Codex sessions."""

from __future__ import annotations

import html
from dataclasses import dataclass


@dataclass(frozen=True)
class SlackRenderedMessage:
    text: str
    blocks: list[dict[str, object]] | None = None
    plain_fallback: bool = False


def render_session_references_html(text: str) -> str:
    lines = str(text).splitlines()
    rendered = [_render_line_html(line, restore_list=_is_restore_notice(text)) for line in lines]
    if not any(changed for _line, changed in rendered):
        return str(text)
    return "\n".join(line for line, _changed in rendered)


def render_session_references_slack(text: str) -> str:
    lines = str(text).splitlines()
    rendered = [_render_line_slack(line, restore_list=_is_restore_notice(text)) for line in lines]
    if not any(changed for _line, changed in rendered):
        return str(text)
    return "\n".join(line for line, _changed in rendered)


def render_session_references_slack_payload(
    text: str,
) -> SlackRenderedMessage:
    rendered_text = render_session_references_slack(text)
    lines = str(text).splitlines()
    restore_list = _is_restore_notice(text)
    if not any(_line_requires_rich_text(line, restore_list=restore_list) for line in lines):
        return SlackRenderedMessage(text=rendered_text)

    elements: list[dict[str, object]] = []
    for index, line in enumerate(lines):
        prefix, label, suffix = _split_session_reference_line(
            line,
            restore_list=restore_list,
        )
        name = (
            _extract_thread_name(label, prefix=prefix, restore_list=restore_list)
            if prefix is not None
            else None
        )
        if prefix is None or name is None:
            _append_rich_text(elements, line)
        else:
            _append_rich_text(elements, prefix)
            elements.append({"type": "text", "text": name, "style": {"bold": True}})
            _append_rich_text(elements, label[len(name) :] + suffix)
        if index + 1 < len(lines):
            _append_rich_text(elements, "\n")
    return SlackRenderedMessage(
        text=str(text),
        blocks=[
            {
                "type": "rich_text",
                "elements": [
                    {
                        "type": "rich_text_section",
                        "elements": elements,
                    }
                ],
            }
        ],
        plain_fallback=True,
    )


def render_restore_notice_html(text: str) -> str:
    return render_session_references_html(text)


def render_restore_notice_slack(text: str) -> str:
    return render_session_references_slack(text)


def _render_line_html(line: str, *, restore_list: bool) -> tuple[str, bool]:
    prefix, label, suffix = _split_session_reference_line(line, restore_list=restore_list)
    if prefix is None:
        return html.escape(line), False
    name = _extract_thread_name(label, prefix=prefix, restore_list=restore_list)
    if name is None:
        return html.escape(line), False
    label_suffix = label[len(name) :]
    return (
        f"{html.escape(prefix)}<b>{html.escape(name)}</b>"
        f"{html.escape(label_suffix)}{html.escape(suffix)}",
        True,
    )


def _render_line_slack(line: str, *, restore_list: bool) -> tuple[str, bool]:
    prefix, label, suffix = _split_session_reference_line(line, restore_list=restore_list)
    if prefix is None:
        return line, False
    name = _extract_thread_name(label, prefix=prefix, restore_list=restore_list)
    if name is None:
        return line, False
    label_suffix = label[len(name) :]
    return f"{prefix}*{_escape_slack_mrkdwn(name)}*{label_suffix}{suffix}", True


def _escape_slack_mrkdwn(text: str) -> str:
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _line_requires_rich_text(line: str, *, restore_list: bool) -> bool:
    prefix, label, _suffix = _split_session_reference_line(line, restore_list=restore_list)
    if prefix is None:
        return False
    name = _extract_thread_name(label, prefix=prefix, restore_list=restore_list)
    return name is not None and any(marker in name for marker in ("*", "_", "~", "`"))


def _append_rich_text(elements: list[dict[str, object]], text: str) -> None:
    if not text:
        return
    if elements and elements[-1].get("type") == "text" and "style" not in elements[-1]:
        elements[-1]["text"] = str(elements[-1].get("text") or "") + text
        return
    elements.append({"type": "text", "text": text})


def _split_session_reference_line(
    line: str,
    *,
    restore_list: bool,
) -> tuple[str | None, str, str]:
    leading = "🧑‍✈️ " if line.startswith("🧑‍✈️ ") else ""
    body = line[len(leading) :]
    for candidate in (
        "Current session: ",
        "thread name: ",
        "session: ",
        "Starting RESUME_AGENT: ",
        "Started RESUME_AGENT: ",
        "Resume failed: ",
    ):
        if not body.startswith(candidate):
            continue
        label = body[len(candidate) :]
        suffix = ""
        if candidate == "Starting RESUME_AGENT: " and " in " in label:
            label, separator, cwd = label.rpartition(" in ")
            suffix = f"{separator}{cwd}"
        return f"{leading}{candidate}", label, suffix
    if restore_list and body.startswith("- "):
        return f"{leading}- ", body[2:], ""
    return None, line, ""


def _extract_thread_name(
    label: str,
    *,
    prefix: str,
    restore_list: bool,
) -> str | None:
    text = str(label).strip()
    if text in {"", "none", "not selected"}:
        return None
    if not (prefix.endswith("Current session: ") or (restore_list and prefix.endswith("- "))):
        if text.startswith("[") and text.endswith("]"):
            return None
        return text

    bracket_start = text.rfind(" [")
    session_id = ""
    prefix = text
    if bracket_start >= 0 and text.endswith("]"):
        session_id = text[bracket_start + 2 : -1].strip()
        prefix = text[:bracket_start].rstrip()
    name = prefix.split(" — ", 1)[0].strip()
    if not name:
        return None
    if name.startswith("[") and name.endswith("]"):
        return None
    if session_id and name == session_id:
        return None
    return name


def _is_restore_notice(text: str) -> bool:
    return str(text).startswith("Context restored after restart.\n")
