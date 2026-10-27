# Plan: prompt-cache misses and missing compaction on large sessions

Source: Railway staging log for Ada on SDK 1.0.6.dev4, 2026-11-04 13:50–13:52, mission
`missions:01m2t6jpd6mvfy6rwp452aw1e0`, model `claude-sonnet-5`, compaction enabled.

## Observed

| Call | input | cache_read | cache_write | Expected |
|------|-------|------------|-------------|----------|
| 1 (first of the run) | 525 537 | 0 | 525 535 | miss is fine (previous run's cache expired) |
| 2 (after 3 tool calls) | 552 384 | **0** | **552 382** | should read ~525k |
| 3 (after 3 tool calls) | 1 372 | 552 382 | 1 370 | ok |

`History measured: estimated=350055 provider=550191 messages=2457 threshold=600000`. Nothing was folded.
Every turn of this mission now sends more than 500k tokens, and call 2 paid for writing the whole
prefix to the cache a second time.

### Full log (run completed at 13:56:03, 321 s, 13 Sonnet calls, no crash)

Each call's `cache_read` equals one earlier call's `cache_read` + `cache_write`. Those sums split
the calls into two chains that never share a prefix:

- Chain A, calls 1 → 4 → 6 → 7 → 10 → 12 → 13: 525 535 → 558 244 → 651 706 → 655 011 → 689 673 → 693 530.
- Chain B, calls 2 → 3 → 5 → 8 → 9 → 11: 552 382 → 553 752 → 617 557 → 658 167 → 689 032.

The calls alternate between **two deployments of `claude-sonnet-5`**, and each has its own cache.
Every hop writes again everything the other deployment saw since its last call (for example call 7
wrote 93k). This confirms the hypothesis below.

The input then grew **inside the run** from 525k to ~695k, past the 600k threshold, with no fold.
The history is only measured once per run, which confirms point 2.2 below.

## 1. Cache miss on call 2

### What the code rules out

- Breakpoints (`SafeLiteLLM._mark_cache_breakpoints`) are on the last tool definition, the system
  prompt and the newest user/tool message. From call 2's breakpoint back to call 1's cached user
  message there are 7 blocks (assistant text, 3 `tool_use`, 3 `tool_result`). That is within
  Anthropic's 20-block lookback, so the breakpoints should have produced a hit.
- No `Context window trim applied` was logged, so `trim_messages` did not rewrite the prefix.
- The tool list did not change: `load_manager` was not called.
- Both calls fall inside the 5-minute TTL (13:51:02 → 13:51:11).

### Log line and billing (SDK)

`CostTrackingLogger` logs `served=<group> deployment=<x-litellm-model-id>` next to `cache_read` and
`cache_write`, read from the response headers (`_hidden_params["additional_headers"]`). It also
**bills on the served group**: today `cost.py:194` bills the requested model, so after a fallback
the tokens are billed at the wrong price. Every token is still counted. The header parsing moves to
a single static method on `LiteLLMRegistry`, shared with `SafeLiteLLM._served_model`.
`[VALIDATE COST-SERVED-MODEL]`.

### Root cause, confirmed by the proxy config (`litellm_proxy_config.yaml`)

- The `claude-sonnet-5` group has **two deployments**:
  - `bedrock/eu.anthropic.claude-sonnet-5` (`weight: 2`)
  - `vertex_ai/claude-sonnet-5` (`weight: 3`)

  With `routing_strategy: simple-shuffle`, each call is drawn at random: 40 % Bedrock, 60 % Vertex.
  Bedrock and Vertex never share a prompt cache, which gives the two chains A and B.
- `optional_pre_call_checks: ["session_affinity", ...]` is enabled, but **it never applies**:
  - LiteLLM's `DeploymentAffinityCheck` reads `metadata.session_id` (`deployment_affinity_check.py:236`).
  - The proxy only fills that field from an `x-litellm-session-id` or `x-litellm-trace-id` header, or
    from a generic `x-<vendor>-session-id` header (`proxy/litellm_pre_call_utils.py:950-955`).
  - The SDK sends the session as a **body field** `extra_body={"litellm_session_id": …}`
    (`safe_litellm.py:166`), and Langfuse as `langfuse_session_id` with no `x-` prefix. Neither
    reaches `metadata.session_id`, so there is no affinity and every call is shuffled again.
  - Caveat: I read the litellm 1.88.1 code installed in Ada's venv. The proxy server's version needs
    checking.
- The proxy's `litellm_settings.cache` (Redis, TTL 600) is LiteLLM's response cache. It has nothing
  to do with Anthropic's prompt cache.

### Target config (priority): Vertex only, one deployment per group

The new proxy config (no `aws_creds`, every Claude group on `vertex_ai/...` only) removes the
cause: `simple-shuffle` has nothing left to choose between, so a mission always lands on the same
cache. **No proxy patch is needed** with this config. `litellm_proxy_config.patched.yaml`
(`deployment_affinity`) only applies to the old Bedrock + Vertex config.

- Evidence that the staging log ran on the **old** config: for the same content, the prompt size
  differs by ~1–2k tokens between chains A and B (call 11 on B at 694 302, then call 12 on A at
  693 532). LiteLLM formats Bedrock (Converse) and Vertex (Anthropic Messages) requests differently,
  which a single Vertex deployment cannot explain.
- Residual risk: `vertex_location: global`. If a run on the new config still shows `cache_read=0`
  mid-run, test a fixed region. Check the regional endpoint pricing first.

### Fix

1. **SDK (one line)**: in `SafeLiteLLM.from_spec`, add `x-litellm-session-id: <session_id>`
   (the mission id) to `extra_headers`, and keep the `litellm_session_id` body field for spend logs.
   Affinity is then keyed per mission and per model group, with `deployment_affinity_ttl_seconds:
   3600`, so a mission stays on one deployment. It does nothing on the Vertex-only config; it keeps
   the pin working if a second deployment comes back. Never send `x-litellm-trace-id` per call: it
   takes priority over `x-litellm-session-id`.
2. **Proxy**: do **not** add `"prompt_caching"`. Its key is the prefix up to the *last*
   `cache_control` marker, and the SDK puts that marker on the newest message, so the key changes on
   every call and never matches. The proxy-only stopgap is the `deployment_affinity` per API key,
   commented in `digitalkin-sandbox/litellm_proxy_config.patched.yaml`. It pins all of one key's
   traffic to one deployment for up to 1 h.
3. **Check after deploying**: the proxy logs `DeploymentAffinityCheck: session-id affinity hit`, and
   in Ada's logs every `cache_read` equals the previous call's `cache_read + cache_write` (one chain).

## 2. Missing compaction

### Why nothing was folded (works as coded)

- `HistoryCompactor.threshold` = `min(DIGITALKIN_COMPACTION_MAX_TOKENS=600_000, window × 0.9)`. On a
  ~1M-window model the threshold is 600k.
- Calibration `(slope 1.343, overhead 55 566)` maps it to ~405k estimated tokens. The estimate was
  350k, so `should_compact` is false. The provider-side 550k is also under 600k.
- The history is measured **once per run** (`_measured` signature). Growth of the current run inside
  a tool loop is never re-measured. It is only caught by `force_fold` once the provider rejects the
  request (`ContextWindowExceededError`, `safe_litellm.py:487`).

So the policy lets a session sit at 500–600k tokens on every turn. Cost, latency and the size of
the session read and written to storage (2 457 messages, a likely source of the container memory
pressure) all scale with that. The cap was sized for the window, not for cost.

### Steps

1. **Threshold as a share of the window, chosen in the setup**: an enum
   `ContextSize(LARGE=0.9, MEDIUM=0.7, SMALL=0.5)` sets threshold = window × ratio.
   `CompactionSettings.max_tokens` is removed, and `window_ratio` (0.9) is the default size.
   `HistoryCompactor.for_model` takes `context_size: ContextSize | None`. Ada adds a `context_size`
   field to its `SetupModel` (`{"config": True}`).

   | Size | Share | 1M window | 200k window | Effect |
   |---|---|---|---|---|
   | LARGE (max) | 90% | 900k | 180k | Most detail kept; most expensive and slowest per call |
   | MEDIUM | 70% | 700k | 140k | Middle ground |
   | SMALL | 50% | 500k | 100k | Cheapest and fastest; old detail is summarized sooner |

   - Breaking: `DIGITALKIN_COMPACTION_MAX_TOKENS` disappears.
   - Unknown window: the threshold falls back to 200k (inline default).
2. **Fold mid-run when the provider's count crosses the threshold** (SDK patch, `compaction.py` only):
   - **Why the provider's count**: it is free, since every response carries it, and exact. A
     `count_tokens` on every call of a tool loop would cost a full tokenization each time.
   - **`HistoryCompactor.__init__`**: add `self._last_provider_tokens: int | None = None` and
     `self._outgrown: tuple | None = None`, the history last folded for this reason (a run has only
     one history at a time).
   - **`observe()`**: today it returns as soon as `_calibrated` is set. Record
     `self._last_provider_tokens = provider_tokens` on every response first, then keep the
     calibration once per run. A fallback's count also counts (it is the real size of the prompt),
     but it is **not saved as a calibration sample**: `token_samples` is persisted per session and is
     specific to the main model's tokenizer.
   - **`view()`**, in the existing lock, after the `if signature not in self._measured:` block, an
     inline `elif` (no helper method): `_last_provider_tokens is not None and
     _last_provider_tokens >= threshold_tokens and signature != _outgrown`. That branch sets
     `_outgrown = signature`, logs `Run outgrew the threshold, folding mid-run: session=… provider=…
     threshold=…`, then runs `record = await self._fold(prefix, history, self._apply(history, record),
     tools, None, run_response) or record`. Folding a history at most once stops a run whose own
     messages fill the context from summarizing on every call.
   - **`input_tokens`** comes from LiteLLM's `prompt_tokens`, cache included
     (`agno/models/litellm/chat.py:534`). The `input=1 372` in the cost line is the uncached part
     only.
   - **`_fold()`**: after a successful fold, reset `_last_provider_tokens = None`, because that count
     was for the unfolded request. The record id changes, so the new signature goes through the normal
     measurement on the next call.
   - **Limits, to accept**: only the replayed history is folded, never the current run (same rule as
     today). A mid-run fold changes the prefix, so the next call rewrites the cache once. On the
     staging log this fold would have fired at call 8 (617k against a 600k threshold), instead of the
     run continuing up to 695k.
   - **Docstrings to update**: the `HistoryCompactor` class ("measured once per distinct history") and
     `observe` ("Once per compactor").
3. **Cold cache at the start of a run: a 1 h TTL rather than an extra fold.** Add an optional
   `ttl: "1h"` on the three existing markers, which Vertex supports natively. After a pause under 1 h,
   the first call reads the cache instead of rewriting 525k. In exchange, every write costs ×2
   instead of ×1.25.
4. **Tests** (`tests/community/agno/test_compaction.py`):
   - threshold for each `ContextSize` on 1M, 200k and unknown windows
   - a fallback response moves `_last_provider_tokens` but adds no calibration sample
   - cost booked on the served group
   - `observe(…, provider=threshold + 1)` then `view()` with the same history: one summary, and a
     third call does not summarize again (`_outgrown`)
   - `observe(…, provider=threshold - 1)` then `view()`: no fold
   - a run with no foldable history (≤ `uncompacted_runs`) returns the request untouched, with no loop
   - after a mid-run fold, `_last_provider_tokens` is reset and `test_calibrates_once_per_run` still passes
   - the calibration mapping is unchanged

## Out of scope / follow-ups

- Ada `agents/agno_agent.py:178`: `_log_toolkits` prints `str(Function)` for the registry managers,
  which is ~15 KB of schema per manager per request. Log `fn.name` instead.
- ~~The container dying silently after 13:52:12~~: there was no crash. The full log shows the run
  completed at 13:56:03 (321 s); the first excerpt was just cut off.

## Order

1. Vertex-only proxy config (done in parallel) + SDK header.
2. Step 2.2 (mid-run fold, without calibrating on fallbacks).
3. Log line and billing on the served model.
4. Step 2.1 (`ContextSize` in the setup), then 2.3 (1 h TTL): policy changes to validate with the
   team.
