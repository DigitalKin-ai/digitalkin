# Agno memory settings

The SDK does not set any Agno history or storage option for you. Every Agno `Agent` or
`Team` built inside a module chooses its own. These are the settings we recommend, and why.

## Recommended settings

```python
from agno.agent import Agent

agent = Agent(
    model=...,
    db=db,                           # a persistent Agno db, shared across turns
    add_history_to_context=True,
    num_history_runs=3,              # bounded history window (Agno's default when unset)
    store_history_messages=False,    # Agno's default; keep it
    max_tool_calls_from_history=3,   # drop old tool results from the context
)
```

| Setting | Recommendation | Why |
| --- | --- | --- |
| `db` | Set it to a persistent Agno db. | History, HITL resume and `ChatHistoryTools` read the session from it. Without a db, history only lives in the process that ran the turn. |
| `num_history_runs` / `num_history_messages` | Keep a small window (for example 3 runs). | Every past message in the window is sent to the model again on each turn. |
| `store_history_messages` | `False` | With `True`, each stored run embeds the full history again, so storage grows quadratically. With `False`, each run stores only its own messages. |
| `max_tool_calls_from_history` | Set it (for example 3). | Tool results are usually the largest messages. Old ones rarely help the model. |

## What the SDK already trims

- **HITL paused runs** (`PausedRunStore`): the stored payload drops the history messages
  (`from_history=True`) and the run `events`. `continue_run` reloads history from the
  session db, so resuming needs a `db`.
- **Module tools** (`ModuleToolkit`): only the last result frame and the last error frame of
  a tool call are kept. The tool's input arguments are not echoed back to the model.

## ChatHistoryTools limits

`ChatHistoryTools` gives the agent an outline-then-read view of the session, with fixed caps:

- `outline_chat_history` returns at most 50 rows when neither `first` nor `last` is given.
  `first` and `last` are capped at 200. With `last`, only the tail of the session is loaded,
  so `total` is `null`.
- `read_chat_messages` reads at most 50 ids per call, and `max_content_chars` is capped at 20000.

When a cap applies, the tool result carries a `note` field so the agent knows to page.
