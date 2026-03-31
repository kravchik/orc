"""Shared Telegram output runtime: status window + send helpers + telemetry."""

from __future__ import annotations

import html
import re
from dataclasses import dataclass
from typing import Callable

from orchestrator import clock
from orchestrator.telegram_status import TelegramStatusClearPlan, TelegramStatusConfig, TelegramStatusWindow


def split_telegram_message(text: str, limit: int = 3900) -> list[str]:
    if limit <= 0 or len(text) <= limit:
        return [text]
    chunks: list[str] = []
    rest = text
    while len(rest) > limit:
        cut = rest.rfind("\n", 0, limit + 1)
        if cut <= 0 or cut < (limit // 2):
            cut = limit
        chunk = rest[:cut]
        if chunk:
            chunks.append(chunk)
        rest = rest[cut:]
        if rest.startswith("\n"):
            rest = rest[1:]
    if rest:
        chunks.append(rest)
    return chunks if chunks else [text]


@dataclass(frozen=True)
class TelegramSendResult:
    chat_id: int | None
    first_message_id: int | None
    sent_chunks: int


@dataclass(frozen=True)
class TelegramDueStatusCandidate:
    status_key: str
    thread_id: int | None
    last_flush_ts: float | None
    delivery_kind: str


@dataclass(frozen=True)
class TelegramQueuedStatusDelete:
    source: str
    status_key: str
    chat_id: int
    message_id: int
    thread_id: int | None
    dropped_pending_lines: int


class TelegramBotStatusBudget:
    def __init__(
        self,
        *,
        logger,
        source: str,
        monotonic_now: Callable[[], float] = clock.monotonic,
        cooldown_sec: float = 2.0,
    ) -> None:
        self._logger = logger
        self._source = source
        self._now = monotonic_now
        self._cooldown_sec = float(cooldown_sec)
        self._status_cooldown_until_ts = 0.0
        self._rate_limit_until_ts = 0.0

    def can_send_any(self) -> bool:
        return float(self._now()) >= self._rate_limit_until_ts

    def can_send_status(self) -> bool:
        now = float(self._now())
        return now >= self._rate_limit_until_ts and now >= self._status_cooldown_until_ts

    def mark_status_sent(self) -> None:
        now = float(self._now())
        self._status_cooldown_until_ts = max(self._status_cooldown_until_ts, now + self._cooldown_sec)
        self._logger.event(
            "telegram_status_global_cooldown_set",
            source=self._source,
            reason="status_sent",
            cooldown_sec=self._cooldown_sec,
            cooldown_until_ts=self._status_cooldown_until_ts,
        )

    def note_rate_limited(self, retry_after_sec: int | None) -> None:
        if retry_after_sec is None or retry_after_sec <= 0:
            return
        now = float(self._now())
        self._rate_limit_until_ts = max(self._rate_limit_until_ts, now + float(retry_after_sec))
        self._logger.event(
            "telegram_status_global_cooldown_set",
            source=self._source,
            reason="telegram_429",
            retry_after_sec=int(retry_after_sec),
            cooldown_until_ts=self._rate_limit_until_ts,
        )


class TelegramKind:
    STATUS = "status"
    REPLY = "reply"
    COMMAND = "command"
    WARNING = "warning"
    APPROVAL_PROMPT = "approval_prompt"
    APPROVAL_DETAILS = "approval_details"
    APPROVAL_ACK = "approval_ack"
    APPROVAL_AUTO = "approval_auto"
    APPROVAL_INVALID = "approval_invalid"
    MIRROR = "mirror"
    NOTIFY = "notify"
    PROMPT = "prompt"
    RESTORE = "restore"
    ERROR = "error"


_KIND_ALIASES = {
    "current": TelegramKind.REPLY,
    "current_error": TelegramKind.ERROR,
    "command_reply": TelegramKind.COMMAND,
    "input_rejected_warning": TelegramKind.WARNING,
    "approval_auto_notify": TelegramKind.APPROVAL_AUTO,
    "approval_invalid_choice": TelegramKind.APPROVAL_INVALID,
    "approval_invalid_response": TelegramKind.APPROVAL_INVALID,
    "human_prompt_mirror": TelegramKind.MIRROR,
    "human_prompt": TelegramKind.PROMPT,
    "human_notify": TelegramKind.NOTIFY,
    "restore_summary": TelegramKind.RESTORE,
}


def normalize_kind(kind: str) -> str:
    raw = (kind or "").strip()
    if raw == "":
        return TelegramKind.REPLY
    return _KIND_ALIASES.get(raw, raw)


class _StatusTelemetryClient:
    """Client proxy used by TelegramStatusWindow to emit unified telegram_op events."""

    def __init__(
        self,
        *,
        base_client,
        logger,
        source: str,
        status_key: str,
        chat_id_getter: Callable[[], int | None],
        target_fields_getter: Callable[[], dict],
        rate_limit_notifier: Callable[[int | None], None] | None = None,
    ) -> None:
        self._base = base_client
        self._logger = logger
        self._source = source
        self._status_key = status_key
        self._chat_id_getter = chat_id_getter
        self._target_fields_getter = target_fields_getter
        self._rate_limit_notifier = rate_limit_notifier

    def send_message(self, chat_id: int, text: str, **kwargs) -> dict:
        payload_text, payload_kwargs = _prepare_status_payload(
            text=text,
            kwargs=kwargs,
            source=self._source,
        )
        try:
            result = self._base.send_message(chat_id=chat_id, text=payload_text, **payload_kwargs)
        except Exception as exc:
            if self._rate_limit_notifier is not None:
                self._rate_limit_notifier(_extract_retry_after_sec(exc))
            raise
        message_id = result.get("message_id") if isinstance(result, dict) else None
        self._logger.event(
            "telegram_op",
            source=self._source,
            op="send",
            kind="status",
            status_key=self._status_key,
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            **self._target_fields_getter(),
        )
        return result

    def edit_message_text(self, chat_id: int, message_id: int, text: str, **kwargs) -> None:
        payload_text, payload_kwargs = _prepare_status_edit_payload(
            text=text,
            kwargs=kwargs,
            source=self._source,
        )
        try:
            self._base.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=payload_text,
                **payload_kwargs,
            )
        except Exception as exc:
            if self._rate_limit_notifier is not None:
                self._rate_limit_notifier(_extract_retry_after_sec(exc))
            raise
        self._logger.event(
            "telegram_op",
            source=self._source,
            op="edit",
            kind="status",
            status_key=self._status_key,
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            **self._target_fields_getter(),
        )

    def delete_message(self, chat_id: int, message_id: int) -> None:
        try:
            self._base.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception as exc:
            if self._rate_limit_notifier is not None:
                self._rate_limit_notifier(_extract_retry_after_sec(exc))
            raise
        self._logger.event(
            "telegram_op",
            source=self._source,
            op="delete",
            kind="status",
            status_key=self._status_key,
            chat_id=chat_id,
            message_id=message_id,
            **self._target_fields_getter(),
        )


class TelegramOutputRuntime:
    """Production shared Telegram output surface for proxy/steward/orchestrator."""

    def __init__(
        self,
        *,
        client,
        logger,
        source: str,
        chat_id_getter: Callable[[], int | None],
        send_kwargs_getter: Callable[[], dict] | None = None,
        status_budget: TelegramBotStatusBudget | None = None,
        status_eager_flush_when_due: bool = True,
        status_config: TelegramStatusConfig = TelegramStatusConfig(),
        status_monotonic_now: Callable[[], float] | None = None,
        split_limit: int = 3900,
    ) -> None:
        self._client = client
        self._logger = logger
        self._source = source
        self._chat_id_getter = chat_id_getter
        self._send_kwargs_getter = send_kwargs_getter or (lambda: {})
        self._status_budget = status_budget
        self._status_eager_flush_when_due = bool(status_eager_flush_when_due)
        self._split_limit = int(split_limit)
        self._status_config = status_config
        self._status_monotonic_now = status_monotonic_now
        self._status_windows: dict[str, TelegramStatusWindow] = {}
        self._latest_status_key: str | None = None

    def append_status(
        self,
        status_text: str,
        *,
        status_key: str | None = None,
        force_new_if_not_latest: bool = False,
    ) -> None:
        key = self._resolve_status_key(
            status_key=status_key,
            force_new_if_not_latest=force_new_if_not_latest,
        )
        self._window_for_key(key).append(status_text)
        self._latest_status_key = key

    def set_status_snapshot(
        self,
        status_text: str,
        *,
        status_key: str | None = None,
        force_new_if_not_latest: bool = False,
    ) -> None:
        key = self._resolve_status_key(
            status_key=status_key,
            force_new_if_not_latest=force_new_if_not_latest,
        )
        self._window_for_key(key).set_snapshot(status_text)
        self._latest_status_key = key

    def clear_status(self) -> None:
        for window in self._status_windows.values():
            window.clear()
        self._status_windows.clear()
        self._latest_status_key = None

    def take_clear_deletes(self) -> list[TelegramQueuedStatusDelete]:
        deletes: list[TelegramQueuedStatusDelete] = []
        target_fields = self._target_fields()
        thread_id = target_fields.get("thread_id")
        normalized_thread_id = thread_id if isinstance(thread_id, int) else None
        for window in self._status_windows.values():
            plan = window.take_clear_plan()
            if plan is None:
                continue
            self._logger.event(
                "telegram_status_clear_queued",
                source=self._source,
                status_key=plan.status_key,
                chat_id=plan.chat_id,
                message_id=plan.message_ids[0],
                message_count=len(plan.message_ids),
                dropped_pending_lines=plan.dropped_pending_lines,
                **target_fields,
            )
            for message_id in plan.message_ids:
                deletes.append(
                    TelegramQueuedStatusDelete(
                        source=self._source,
                        status_key=plan.status_key,
                        chat_id=plan.chat_id,
                        message_id=message_id,
                        thread_id=normalized_thread_id,
                        dropped_pending_lines=plan.dropped_pending_lines,
                    )
                )
        self._status_windows.clear()
        self._latest_status_key = None
        return deletes

    def status_keys(self) -> list[str]:
        return list(self._status_windows.keys())

    def discard_pending_status_updates(self) -> None:
        for window in self._status_windows.values():
            window.discard_pending()

    def flush_status(self, *, status_key: str | None = None) -> bool:
        key = status_key if status_key is not None else self._latest_status_key
        if not isinstance(key, str) or not key:
            return False
        window = self._status_windows.get(key)
        if window is None:
            return False
        window.flush_pending(reason="manual")
        return True

    def flush_due_statuses(self) -> bool:
        progressed = False
        for window in self._status_windows.values():
            if window.flush_due(reason="due"):
                progressed = True
        return progressed

    def peek_due_status_candidate(self) -> TelegramDueStatusCandidate | None:
        now = float((self._status_monotonic_now or clock.monotonic)())
        best: TelegramDueStatusCandidate | None = None
        for status_key, window in self._status_windows.items():
            if not window.has_due_pending(now=now):
                continue
            candidate = TelegramDueStatusCandidate(
                status_key=status_key,
                thread_id=self._target_fields().get("thread_id"),
                last_flush_ts=window.scheduling_last_flush_ts(),
                delivery_kind=window.pending_delivery_kind() or "edit",
            )
            if best is None:
                best = candidate
                continue
            current_score = float("-inf") if candidate.last_flush_ts is None else float(candidate.last_flush_ts)
            best_score = float("-inf") if best.last_flush_ts is None else float(best.last_flush_ts)
            if current_score < best_score or (current_score == best_score and candidate.status_key < best.status_key):
                best = candidate
        return best

    def status_delivery_kind(self, *, status_key: str | None = None) -> str | None:
        key = status_key if status_key is not None else self._latest_status_key
        if not isinstance(key, str) or not key:
            return None
        window = self._status_windows.get(key)
        if window is None:
            return None
        return window.pending_delivery_kind()

    def _window_for_key(self, key: str) -> TelegramStatusWindow:
        window = self._status_windows.get(key)
        if window is not None:
            return window
        status_client = _StatusTelemetryClient(
            base_client=self._client,
            logger=self._logger,
            source=self._source,
            status_key=key,
            chat_id_getter=self._chat_id_getter,
            target_fields_getter=self._target_fields,
            rate_limit_notifier=self._note_rate_limited,
        )
        window = TelegramStatusWindow(
            client=status_client,
            logger=self._logger,
            source=self._source,
            status_key=key,
            chat_id_getter=self._chat_id_getter,
            target_fields_getter=self._target_fields,
            send_kwargs_getter=self._send_kwargs_getter,
            eager_flush_when_due=self._status_eager_flush_when_due,
            rate_limit_notifier=self._note_rate_limited,
            config=self._status_config,
            monotonic_now=self._status_monotonic_now or clock.monotonic,
        )
        self._status_windows[key] = window
        return window

    def _resolve_status_key(
        self,
        *,
        status_key: str | None,
        force_new_if_not_latest: bool,
    ) -> str:
        if status_key is not None:
            return status_key
        if not force_new_if_not_latest:
            return "default"
        return "aux"

    def _target_fields(self) -> dict:
        kwargs = dict(self._send_kwargs_getter() or {})
        thread_id = kwargs.get("message_thread_id")
        return {"thread_id": thread_id if isinstance(thread_id, int) else None}

    def _note_rate_limited(self, retry_after_sec: int | None) -> None:
        if self._status_budget is not None:
            self._status_budget.note_rate_limited(retry_after_sec)

    def send_text(
        self,
        *,
        text: str,
        kind: str,
        chat_id: int | None = None,
        reply_markup: dict | None = None,
        parse_mode: str | None = None,
        raise_on_error: bool = False,
    ) -> TelegramSendResult:
        target_chat_id = chat_id if chat_id is not None else self._chat_id_getter()
        normalized_kind = normalize_kind(kind)
        if target_chat_id is None:
            self._logger.event(
                "telegram_response_skipped_no_chat",
                source=self._source,
                kind=normalized_kind,
                text=text[:200],
                **self._target_fields(),
            )
            return TelegramSendResult(chat_id=None, first_message_id=None, sent_chunks=0)

        prepared_text, prepared_parse_mode = _prepare_outgoing_message(
            text=text,
            kind=normalized_kind,
            parse_mode=parse_mode,
        )
        chunks = [prepared_text] if (reply_markup is not None or prepared_parse_mode is not None) else split_telegram_message(
            prepared_text, limit=self._split_limit
        )
        first_message_id: int | None = None
        sent_chunks = 0
        for index, chunk in enumerate(chunks, start=1):
            kwargs: dict = dict(self._send_kwargs_getter() or {})
            kwargs.update(_send_message_kwargs_for_kind(normalized_kind))
            if index == 1 and reply_markup is not None:
                kwargs["reply_markup"] = reply_markup
            if prepared_parse_mode is not None:
                kwargs["parse_mode"] = prepared_parse_mode
            try:
                sent = self._client.send_message(chat_id=target_chat_id, text=chunk, **kwargs)
            except Exception as exc:
                retry_after_sec = _extract_retry_after_sec(exc)
                self._note_rate_limited(retry_after_sec)
                self._logger.event(
                    "telegram_response_error",
                    source=self._source,
                    kind=normalized_kind,
                    chat_id=target_chat_id,
                    chunk_index=index,
                    chunks_total=len(chunks),
                    error=str(exc),
                    error_type=type(exc).__name__,
                    retry_after_sec=retry_after_sec,
                    text=chunk[:200],
                    **self._target_fields(),
                )
                self._logger.event(
                    "telegram_op",
                    source=self._source,
                    op="send_error",
                    kind=normalized_kind,
                    chat_id=target_chat_id,
                    chunk_index=index,
                    retry_after_sec=retry_after_sec,
                    **self._target_fields(),
                )
                if raise_on_error:
                    raise
                return TelegramSendResult(
                    chat_id=target_chat_id,
                    first_message_id=first_message_id,
                    sent_chunks=sent_chunks,
                )
            sent_chunks += 1
            if first_message_id is None and isinstance(sent, dict):
                mid = sent.get("message_id")
                if isinstance(mid, int):
                    first_message_id = mid
            self._logger.event(
                "telegram_op",
                source=self._source,
                op="send",
                kind=normalized_kind,
                chat_id=target_chat_id,
                message_id=sent.get("message_id") if isinstance(sent, dict) else None,
                chunk_index=index,
                chunks_total=len(chunks),
                text=chunk,
                **self._target_fields(),
            )
            self._logger.event(
                "telegram_response_sent",
                source=self._source,
                kind=normalized_kind,
                chat_id=target_chat_id,
                message_id=sent.get("message_id") if isinstance(sent, dict) else None,
                chunk_index=index,
                chunks_total=len(chunks),
                text=chunk,
                has_reply_markup=reply_markup is not None and index == 1,
                **self._target_fields(),
            )
        return TelegramSendResult(
            chat_id=target_chat_id,
            first_message_id=first_message_id,
            sent_chunks=sent_chunks,
        )


def _prepare_outgoing_message(*, text: str, kind: str, parse_mode: str | None) -> tuple[str, str | None]:
    if kind != TelegramKind.APPROVAL_PROMPT:
        return text, parse_mode
    return _render_expandable_block_message(title="Approval needed", body_text=text), "HTML"


def _send_message_kwargs_for_kind(kind: str) -> dict[str, object]:
    if kind == TelegramKind.RESTORE:
        return {"disable_notification": True}
    return {}


def _prepare_status_payload(*, text: str, kwargs: dict, source: str) -> tuple[str, dict]:
    payload_kwargs = dict(kwargs)
    payload_kwargs["parse_mode"] = "HTML"
    payload_kwargs["disable_notification"] = True
    return _render_expandable_status_message(text=text, source=source), payload_kwargs


def _prepare_status_edit_payload(*, text: str, kwargs: dict, source: str) -> tuple[str, dict]:
    payload_kwargs = dict(kwargs)
    payload_kwargs["parse_mode"] = "HTML"
    payload_kwargs.pop("disable_notification", None)
    return _render_expandable_status_message(text=text, source=source), payload_kwargs


def _render_expandable_status_message(*, text: str, source: str) -> str:
    lines = str(text).splitlines()
    status_icon = _status_icon_for_source(source)
    status_header = f"{status_icon} [status]" if status_icon else "[status]"
    if not lines:
        return status_header
    body_start = 0
    if lines[0] == "[status]":
        body_start = 1
    header = status_header
    body_lines = [line for line in lines[body_start:] if line.strip()]
    if not body_lines:
        return _escape_html(header)
    body = _render_status_body_html(body_lines)
    return f"{_escape_html(header)}\n<blockquote expandable>{body}</blockquote>"


def _status_icon_for_source(source: str) -> str:
    if source in {"steward", "telegram_steward", "slack_steward"}:
        return "🧑‍✈️"
    if source in {"agent", "telegram_agent", "slack_agent"}:
        return "🚀"
    return ""


def _render_status_body_html(lines: list[str]) -> str:
    rendered: list[str] = []
    marker = "agentCommentary: "
    for line in lines:
        if line.startswith(marker):
            message = line[len(marker) :].strip()
            if message:
                rendered.append(f"<i>{_escape_html(message)}</i>")
            continue
        rendered.append(_render_status_line_html(line))
    return "\n".join(rendered)


def _render_status_line_html(line: str) -> str:
    text = line.strip()
    if text.startswith("commandExecution "):
        return f"&gt; <code>{_escape_html(text)}</code>"
    if text.startswith("webSearch ") or text.startswith("webFetch "):
        return f"🌐 <code>{_escape_html(text)}</code>"
    return _escape_html(text)


def _render_expandable_block_message(*, title: str, body_text: str) -> str:
    lines = [line.rstrip() for line in str(body_text).splitlines() if line.strip()]
    if not lines:
        return _escape_html(title)
    if len(lines) == 1:
        return _escape_html(lines[0])
    body = "\n".join(lines[1:])
    return f"{_escape_html(lines[0])}\n<blockquote expandable>{_escape_html(body)}</blockquote>"


def _escape_html(value: str) -> str:
    return html.escape(value, quote=False)


def _extract_retry_after_sec(exc: Exception) -> int | None:
    match = re.search(r"retry after (\d+)", str(exc), flags=re.IGNORECASE)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None
