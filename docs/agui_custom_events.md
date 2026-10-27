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

## Mission title

`name: "mission_title"`, `value: {"title": "<title>" | null}`. Emitted at most once per mission, never updated afterwards (a manual rename is the back's business).

- A trigger proposes the title by overriding `AgUiMixin.mission_title(context) -> str | None`. Without an override, no title task runs.
- The hook runs as a background task on each `RUN_STARTED` until the mission has a title: `RUN_FINISHED` waits for it, `RUN_ERROR` cancels it.
- A None or blank result means "not enough context yet": nothing is emitted (the back shows the user prompt) and the next run tries again.
- The run that reaches `DIGITALKIN_AGUI_MISSION_TITLE_ATTEMPTS` empty results (default 3) emits `{"title": null}`: every attempt is used, no title will come, the back keeps its own default. The hook is not called again.
- A hook that raises is logged and counts no attempt.
- State lives in the SDK-owned `mission` storage collection (record `mission`, `MissionRecord`: `title`, `title_attempts`), mission-scoped and registered on every module by `ServicesConfig`.

## Sources

Sources are no longer a custom event: they travel as the `sources` activity ([`agui_events.md#sources`](agui_events.md#sources)). The `source_citation` name is still accepted as an *input* (agno `CustomEvent`, `SourceCitationEvent`) and converted.
