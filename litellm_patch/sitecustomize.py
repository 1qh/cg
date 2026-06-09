"""
Version-resilient runtime patch for LiteLLM.

Problem: LiteLLM's `_ensure_tool_results_have_corresponding_tool_calls` (which rebuilds the
assistant `tool_calls` so a tool result isn't orphaned) only runs on the stateful
`previous_response_id` path. Codex CLI sends the FULL input array statelessly every turn, which
goes through `transform_responses_api_input_to_messages` — where the fix is NOT wired in.
Result: Gemini/Anthropic reject with "Missing corresponding tool call".

This file monkeypatches the stateless transform to post-process its output with the existing
fix. It is:
  - line-number independent (binds by attribute name, not source edits)
  - idempotent (guard flag)
  - graceful: if upstream renames/removes/fixes anything, it no-ops with a log line instead
    of crashing — so it is safe to leave mounted across every future LiteLLM version.

Loaded automatically: Python imports `sitecustomize` at interpreter startup when it is on
PYTHONPATH. Mount this dir and set PYTHONPATH=/patch — works on any litellm image tag, no rebuild.
"""
import functools
import os
import sys

_FLAG = "_codex_gemini_toolcall_patch_applied"


def _log(msg):
    sys.stderr.write(f"[litellm-patch] {msg}\n")
    sys.stderr.flush()


def _apply():
    try:
        from litellm.responses.litellm_completion_transformation.transformation import (
            LiteLLMCompletionResponsesConfig as Cfg,
        )
    except Exception as e:  # litellm absent or module moved -> nothing to patch
        _log(f"skip: cannot import transformation module ({e})")
        return

    if getattr(Cfg, _FLAG, False):
        return  # already patched this process

    orig = getattr(Cfg, "transform_responses_api_input_to_messages", None)
    ensure = getattr(Cfg, "_ensure_tool_results_have_corresponding_tool_calls", None)
    if orig is None or ensure is None:
        # Upstream changed the API (maybe they fixed it). Do nothing, stay safe.
        _log("skip: target methods not found (upstream changed or already fixed)")
        return

    raw_orig = orig.__func__ if hasattr(orig, "__func__") else orig
    raw_ensure = ensure.__func__ if hasattr(ensure, "__func__") else ensure

    @staticmethod
    @functools.wraps(raw_orig)
    def patched(input, responses_api_request, *args, **kwargs):
        messages = raw_orig(input, responses_api_request, *args, **kwargs)
        try:
            tools = None
            if isinstance(responses_api_request, dict):
                tools = responses_api_request.get("tools")
            else:
                getter = getattr(responses_api_request, "get", None)
                if callable(getter):
                    tools = getter("tools")
            messages = raw_ensure(messages=messages, tools=tools or [])
        except Exception as e:
            # Never break the request because of the patch.
            _log(f"ensure step failed, passing through unmodified ({e})")
        return messages

    Cfg.transform_responses_api_input_to_messages = patched
    setattr(Cfg, _FLAG, True)
    _log("applied: stateless tool_call reconstruction wired in")


_GS_FLAG = "_codex_gemini_grounding_injected"


def _apply_grounding():
    """Inject Gemini Google Search grounding into EVERY generateContent request, alongside
    codex's function tools. Gemini allows built-in + function tools together when
    toolConfig.includeServerSideToolInvocations=True. Result: every codex turn gets
    server-side grounding automatically — no MCP, no model tool-selection, deterministic."""
    if os.environ.get("LITELLM_INJECT_GROUNDING") != "1":
        return
    try:
        import litellm.llms.vertex_ai.gemini.transformation as gt
    except Exception as e:
        _log(f"grounding skip: cannot import gemini transformation ({e})")
        return
    if getattr(gt, _GS_FLAG, False):
        return
    orig = getattr(gt, "_transform_request_body", None)
    if orig is None:
        _log("grounding skip: _transform_request_body not found")
        return

    @functools.wraps(orig)
    def patched_body(*args, **kwargs):
        data = orig(*args, **kwargs)
        try:
            if isinstance(data, dict) and "contents" in data:
                tools = data.get("tools")
                if not isinstance(tools, list):
                    tools = []
                if not any(isinstance(t, dict) and "google_search" in t for t in tools):
                    tools.append({"google_search": {}})
                if not any(isinstance(t, dict) and "url_context" in t for t in tools):
                    tools.append({"url_context": {}})
                data["tools"] = tools
                tc = data.get("toolConfig")
                if not isinstance(tc, dict):
                    tc = {}
                tc["includeServerSideToolInvocations"] = True
                data["toolConfig"] = tc
        except Exception as e:
            _log(f"grounding inject failed, passing through ({e})")
        return data

    gt._transform_request_body = patched_body
    setattr(gt, _GS_FLAG, True)
    _log("applied: google_search grounding injected into every Gemini request")


_RS_FLAG = "_codex_gemini_reasoning_summary_fixed"


def _apply_reasoning_summary_fix():
    """Fix the /responses reasoning-summary SSE stream so codex app-server doesn't HANG when the
    model is told `supports_reasoning_summaries:true`.

    litellm's LiteLLMCompletionStreamingIterator emits `reasoning_summary_text.delta` (and later
    `.done`/`part.done`) but NEVER emits the opening `response.reasoning_summary_part.added`, and
    it stamps each delta with `item_id=f"rs_{hash(delta)}"` — a different id every chunk that also
    mismatches the id used by the done events. codex's responses parser opens a summary part on
    `part.added` and correlates by item_id; with no `part.added` and inconsistent ids it never
    closes the reasoning item, so the turn never completes (exec tolerates it; app-server hangs).

    This wraps the per-chunk transform to (1) stamp every reasoning delta with the iterator's
    stable reasoning item id, and (2) emit exactly one `response.reasoning_summary_part.added`
    (correct id) immediately before the first delta. Graceful no-op if upstream changes."""
    try:
        import importlib
        # NOTE: `import litellm.responses.x.y as si` fails because litellm resolves `responses`
        # lazily (its __path__ is None at statement-resolution time). importlib forces a proper
        # package import and works.
        si = importlib.import_module(
            "litellm.responses.litellm_completion_transformation.streaming_iterator"
        )
        from litellm.types.llms.openai import (
            ResponsePartAddedEvent,
            ResponsesAPIStreamEvents,
            OutputItemAddedEvent,
            BaseLiteLLMOpenAIResponseObject,
        )
        import uuid as _uuid
    except Exception as e:
        _log(f"reasoning-summary skip: cannot import streaming iterator ({e})")
        return

    Cls = getattr(si, "LiteLLMCompletionStreamingIterator", None)
    if Cls is None or getattr(Cls, _RS_FLAG, False):
        return
    orig = getattr(Cls, "_transform_chat_completion_chunk_to_response_api_chunk", None)
    if orig is None:
        _log("reasoning-summary skip: target method not found")
        return

    DELTA = ResponsesAPIStreamEvents.REASONING_SUMMARY_TEXT_DELTA
    ADDED = ResponsesAPIStreamEvents.RESPONSE_PART_ADDED
    OUT_DELTA = ResponsesAPIStreamEvents.OUTPUT_TEXT_DELTA
    OUT_ITEM_ADDED = ResponsesAPIStreamEvents.OUTPUT_ITEM_ADDED

    @functools.wraps(orig)
    def patched(self, chunk):
        ev = orig(self, chunk)
        try:
            if ev is not None and getattr(ev, "type", None) == DELTA:
                rid = (
                    getattr(self, "_reasoning_item_id", None)
                    or getattr(self, "_cached_reasoning_item_id", None)
                )
                if rid:
                    try:
                        ev.item_id = rid
                    except Exception:
                        ev.__dict__["item_id"] = rid
                if not getattr(self, "_sent_reasoning_summary_part_added_event", False):
                    self._sent_reasoning_summary_part_added_event = True
                    part_added = ResponsePartAddedEvent(
                        type=ADDED,
                        item_id=rid or "rs_0",
                        output_index=0,
                        part={"type": "summary_text", "text": ""},
                    )
                    # FIFO _pending_response_events: appended here, the caller appends the delta
                    # right after -> part.added is emitted immediately before the first delta.
                    self._pending_response_events.append(part_added)
            # When reasoning precedes a message, the iterator already set
            # sent_output_item_added_event for the REASONING item, so the MESSAGE that follows
            # gets output_text.delta with NO output_item.added/content_part.added -> codex errors
            # "OutputTextDelta without active item". Open the message item before its first delta.
            elif ev is not None and getattr(ev, "type", None) == OUT_DELTA:
                if getattr(self, "_reasoning_done_emitted", False) and not getattr(
                    self, "_patched_msg_item_opened", False
                ):
                    self._patched_msg_item_opened = True
                    mid = getattr(self, "_cached_item_id", None)
                    if not mid or str(mid).startswith("rs_"):
                        mid = f"msg_{_uuid.uuid4()}"
                        self._cached_item_id = mid
                    try:
                        ev.item_id = mid
                    except Exception:
                        ev.__dict__["item_id"] = mid
                    item_added = OutputItemAddedEvent(
                        type=OUT_ITEM_ADDED,
                        output_index=0,
                        item=BaseLiteLLMOpenAIResponseObject(
                            **{
                                "id": mid,
                                "type": "message",
                                "status": "in_progress",
                                "role": "assistant",
                                "content": [],
                            }
                        ),
                    )
                    cpa = self.create_content_part_added_event()
                    self.sent_content_part_added_event = True
                    # order: output_item.added(message) -> content_part.added -> (caller appends) delta
                    self._pending_response_events.append(item_added)
                    self._pending_response_events.append(cpa)
        except Exception as e:
            _log(f"reasoning-summary inject failed, passing through ({e})")
        return ev

    Cls._transform_chat_completion_chunk_to_response_api_chunk = patched
    setattr(Cls, _RS_FLAG, True)
    _log("applied: reasoning_summary_part.added + message-item-after-reasoning + delta item_id")


_RE_FLAG = "_codex_gemini_effort_clamped"


def _apply_reasoning_effort_clamp():
    """Clamp unknown Gemini reasoning_effort values to 'high' instead of 500-ing.

    codex sends `reasoning_effort: "xhigh"` (extra-high) whenever the effort isn't pinned in config
    — notably on its COMPACTION summarization calls, and on any turn that doesn't set
    `model_reasoning_effort`. litellm's Gemini mapping only knows
    minimal/low/medium/high/disable/none and raises `ValueError: Invalid reasoning effort: xhigh`
    -> HTTP 500, so those turns (and every compaction) fail. This wraps both effort->thinking
    mappers to clamp any unrecognized effort to 'high' (Gemini's max). Graceful no-op if upstream
    changes. Makes the stack robust to whatever effort string codex emits."""
    try:
        import importlib
        m = importlib.import_module(
            "litellm.llms.vertex_ai.gemini.vertex_and_google_ai_studio_gemini"
        )
        VC = getattr(m, "VertexGeminiConfig", None)
    except Exception as e:
        _log(f"effort-clamp skip: cannot import gemini config ({e})")
        return
    if VC is None or getattr(VC, _RE_FLAG, False):
        return
    KNOWN = {"minimal", "low", "medium", "high", "disable", "none"}

    def make(raw):
        @staticmethod
        @functools.wraps(raw)
        def wrapped(reasoning_effort, model=None):
            if reasoning_effort is not None and reasoning_effort not in KNOWN:
                _log(f"clamped reasoning_effort {reasoning_effort!r} -> 'high'")
                reasoning_effort = "high"
            return raw(reasoning_effort, model)
        return wrapped

    patched_any = False
    for name in (
        "_map_reasoning_effort_to_thinking_budget",
        "_map_reasoning_effort_to_thinking_level",
    ):
        orig = getattr(VC, name, None)
        if orig is None:
            continue
        raw = orig.__func__ if hasattr(orig, "__func__") else orig
        setattr(VC, name, make(raw))
        patched_any = True
    if patched_any:
        setattr(VC, _RE_FLAG, True)
        _log("applied: reasoning_effort clamp (unknown -> high)")
    else:
        _log("effort-clamp skip: mapping methods not found")


_RL_FLAG = "_codex_gemini_req_logged"


def _apply_request_logger():
    """DIAGNOSTIC (env LITELLM_LOG_REQ=1): append a one-line summary of every Gemini generateContent
    request to /tmp/req.log — last user-text snippet + #contents + whether it looks like a compaction/
    summary prompt. Used to see if codex re-sends the SAME summarization request during a compaction
    hang (stuck handshake). Off by default."""
    if os.environ.get("LITELLM_LOG_REQ") != "1":
        return
    try:
        import importlib
        gt = importlib.import_module("litellm.llms.vertex_ai.gemini.transformation")
    except Exception as e:
        _log(f"req-logger skip: {e}")
        return
    if getattr(gt, _RL_FLAG, False):
        return
    orig = getattr(gt, "_transform_request_body", None)
    if orig is None:
        return

    @functools.wraps(orig)
    def patched(*args, **kwargs):
        data = orig(*args, **kwargs)
        try:
            contents = data.get("contents") if isinstance(data, dict) else None
            n = len(contents) if isinstance(contents, list) else 0
            si = data.get("systemInstruction") or data.get("system_instruction") or {}
            sys_txt = ""
            try:
                sys_txt = " ".join(pp.get("text", "") for pp in (si.get("parts") or []))
            except Exception:
                pass
            last = ""
            if isinstance(contents, list) and contents:
                for pp in (contents[-1].get("parts") or []):
                    last += pp.get("text", "")
            mark = "COMPACT?" if any(k in (sys_txt + last).lower() for k in ("summar", "compact", "condense", "earlier conversation")) else "turn"
            with open("/tmp/req.log", "a") as f:
                f.write(f"{mark}\tcontents={n}\tlast={last[:80]!r}\tsys={sys_txt[:60]!r}\n")
            with open("/tmp/sys.log", "a") as f:
                f.write(f"=== sys ({len(sys_txt)} chars) ===\n{sys_txt}\n")
        except Exception:
            pass
        return data

    gt._transform_request_body = patched
    setattr(gt, _RL_FLAG, True)
    _log("applied: request logger (diagnostic)")


def _selfcheck():
    """Verify each CRITICAL patch actually applied (its flag is set). The patches no-op gracefully on
    upstream change, which would SILENTLY degrade features — this turns that silence into a loud,
    consolidated status line. Set LITELLM_PATCH_STRICT=1 to make a missing critical patch HARD-FAIL the
    proxy at startup (recommended for production, so a litellm upgrade can't ship a half-working bridge)."""
    import importlib
    checks = []
    def probe(label, modpath, clsname, flag, critical=True):
        try:
            m = importlib.import_module(modpath)
            obj = getattr(m, clsname) if clsname else m
            ok = bool(getattr(obj, flag, False))
        except Exception:
            ok = False
        checks.append((label, ok, critical))
    probe("toolcall", "litellm.responses.litellm_completion_transformation.transformation", "LiteLLMCompletionResponsesConfig", _FLAG)
    probe("reasoning_summary+msg_item", "litellm.responses.litellm_completion_transformation.streaming_iterator", "LiteLLMCompletionStreamingIterator", _RS_FLAG)
    probe("effort_clamp", "litellm.llms.vertex_ai.gemini.vertex_and_google_ai_studio_gemini", "VertexGeminiConfig", _RE_FLAG)
    # grounding only critical when explicitly enabled
    if os.environ.get("LITELLM_INJECT_GROUNDING") == "1":
        probe("grounding", "litellm.llms.vertex_ai.gemini.transformation", None, _GS_FLAG)
    missing = [l for (l, ok, crit) in checks if not ok and crit]
    status = " ".join(f"{l}={'OK' if ok else 'MISSING'}" for (l, ok, _) in checks)
    _log(f"PATCH SELF-CHECK: {status}")
    if missing:
        msg = f"CRITICAL patches MISSING: {missing} — bridge would silently degrade (likely a litellm version change)."
        _log(msg)
        if os.environ.get("LITELLM_PATCH_STRICT") == "1":
            # A bare `raise` is swallowed by litellm's startup, so the proxy serves degraded anyway.
            # os._exit force-terminates the process so the bridge genuinely fails-fast and a litellm
            # upgrade can't ship a half-working bridge unnoticed.
            _log(f"LITELLM_PATCH_STRICT=1 -> hard-exiting the proxy. {msg}")
            sys.stderr.flush()
            os._exit(97)
    else:
        _log("PATCH SELF-CHECK: all critical patches active ✓")


if os.environ.get("LITELLM_DISABLE_CODEX_PATCH") != "1":
    _apply()
    _apply_grounding()
    _apply_reasoning_summary_fix()
    _apply_reasoning_effort_clamp()
    _apply_request_logger()
    _selfcheck()
