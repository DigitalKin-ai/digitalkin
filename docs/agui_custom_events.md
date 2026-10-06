# AG-UI custom events

AG-UI has no dedicated event type for application data. Its extension point is `CUSTOM` (`name` + `value`); every AG-UI client receives it and routes on `name`. DigitalKin ships every `CUSTOM` event under the `agui_custom` protocol.

This page covers `CUSTOM` only; every other AG-UI event and the pipeline state are in [`agui_events.md`](agui_events.md).

## Envelope

```json
{"root": {"protocol": "agui_custom", "createdAt": "2026-09-29T10:14:03.512Z",
  "event": {"type": "CUSTOM", "name": "<event name>", "value": {…},
            "subagentRunId": "…", "metadata": {"digitalkin": {…}}}},
 "annotations": {}}
```

- AG-UI fields are camelCase (`subagentRunId`); keys inside `value` are passed as-is.
- `subagentRunId` and `metadata` are present only when the event was produced inside a delegated subagent run. Group on `subagentRunId`, like text and tool events.
- Null fields are omitted.

## `source_citation`

One source the agent used to answer. One event per source.

| Field | Type | Description |
|-------|------|-------------|
| `url` | `string` (http/https URI) | Link to the source. Always present. |
| `title` | `string`, optional | Short label. Same field as agno's `UrlCitation.title`. |
| `description` | `string`, optional | What the source supports in the answer. |

```json
{"type": "CUSTOM", "name": "source_citation",
 "value": {"url": "https://docs.digitalkin.ai/pricing",
           "title": "Pricing – DigitalKin",
           "description": "Pro plan is 49 €/month"}}
```

JSON Schema of `value` (`digitalkin.models.events.SourceCitation`):

```json
{"title": "SourceCitation", "type": "object", "required": ["url"],
 "properties": {
   "url": {"type": "string", "format": "uri", "minLength": 1, "maxLength": 2083},
   "title": {"anyOf": [{"type": "string"}, {"type": "null"}]},
   "description": {"anyOf": [{"type": "string"}, {"type": "null"}]}}}
```

### Front rules

- Deduplicate on `url` within a run. Model-native citations are deduplicated server-side, explicit tool citations are not.
- Sources arrive progressively, before or while the answer streams, and always before `RUN_FINISHED`.
- Render under the assistant message (or the subagent's, via `subagentRunId`): link labelled by `title` (fallback: host of `url`), `description` as secondary text.
- A bare host is normalised with a trailing slash (`https://a.io` → `https://a.io/`).

### Emitting

| Source | How |
|--------|-----|
| Model-native (OpenAI Responses, Claude, Gemini grounding, Perplexity web search) | Automatic: `AgnoStreamAdapter` forwards agno's `citations.urls` as `url` + `title`. |
| `DkToolkit` subclass | `await self._cite(url, title=..., description=...)` |
| Any agno tool | `yield CustomEvent(name="source_citation", value={"url": ..., "title": ..., "description": ...})` (`agno.run.agent.CustomEvent`) |
| `AgUiMixin` trigger | `await self.send_message(context, SourceCitationEvent(value=SourceCitation(url=..., description=...)))` |
| Remote tool module | Emit it on the tool's own stream; `ModuleToolkit` relays `agui_custom` to the calling agent. |

Invalid sources (non-http URL, missing `url`) are logged and dropped; they never fail the run.
