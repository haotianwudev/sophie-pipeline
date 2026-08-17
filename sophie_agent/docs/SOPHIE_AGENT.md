# Sophie Agent — design doc

A LangChain tool-calling agent over the Sophie platform: 240 wiki pages, a 19-table market-data
Postgres, the public GraphQL API, live SPX option chains, and 14 years of historical OptionsDX
chains. Lives at `sophie_agent/`, entrypoints `sophie_agent/run.py` (CLI) and
`sophie_agent/serve.py` (local-only AG-UI HTTP API, Phase 2 — see below). The Sophie client repo's
chat widget (`src/components/chat/`) talks to the latter directly.

## Why

Everything above was queryable only through bespoke scripts, notebook cells, or copy/paste into
Gemini. `sophie-pipeline` already had LangChain installed but had never used tool calling — its 15
analyst "agents" are single-shot structured-output LLM calls inside a fan-out LangGraph, not
tool-using agents. This package is greenfield, built on top of the existing LLM plumbing
(`src/llm/models.py`), DB helper (`src/tools/api_db.py`), and Cboe client (`src/tools/api_cboe.py`).

## Primary use case: strategy leg selection

*"How do I select the options for an iron condor right now?"* Sophie holds three ingredients of a
real answer that had never been joined:

| Ingredient | Where it lives | Toolkit |
|---|---|---|
| What the strategy *is*, conventional delta/DTE choices | `public/wiki/option-strategy/*.md` | Wiki |
| What deltas/DTE **actually worked** — Sharpe, max DD, win rate | `option_research_run.params`/`.metrics` in Postgres | MarketData |
| Which real contracts satisfy those targets today | Cboe live chain / historical parquet | OptionChain |
| Resolving targets → concrete legs, pricing the structure | ported preset + payoff math | Strategy |

The site's iron condor preset (0.10-delta wings, 0.16-delta shorts) came directly from
`sophie-option-research/configs/iron_condor_45dte.yaml` — the only preset in `presets.ts` that is
backtest-derived rather than a conventional retail default. Making that distinction legible to the
user is the agent's central job: wiki explains, backtest justifies, live chain instantiates.

## The four layers

```
┌─ DELEGATION ──────────────────────────────────────────────────────────┐
│  AgentProfile registry · AgentRuntime · DelegationToolkit             │
│  "which specialist, how many, in parallel or not"                     │
└───────────────────────────────┬───────────────────────────────────────┘
                                │ spins up N of ↓
┌─ BACKBONE ────────────────────┴───────────────────────────────────────┐
│  SophieAgent = LLM + assembled prompt + tool-calling loop + history    │
│  One class. Provider-agnostic. Profile-driven. Knows nothing about     │
│  finance — all domain knowledge arrives through the two layers below.  │
└──────────────┬──────────────────────────────────┬─────────────────────┘
               │ calls                            │ reads/writes
┌─ TOOLKITS ───┴──────────────────┐  ┌─ CONTEXT ──┴──────────────────────┐
│  What the agent can DO          │  │  What the agent KNOWS unasked      │
│  Wiki · OptionChain · Strategy  │  │  RunContext(as_of) · prompt        │
│  DataFrame · MarketData         │  │  assembly · DataFrameStore ·       │
│  (each a BaseToolkit subclass)  │  │  chat history                      │
└─────────────────────────────────┘  └────────────────────────────────────┘
   ╰──────────── cross-cutting: point-in-time · deterministic math ·
                 typed outputs · evals · tracing · caching · run records
```

Toolkits are capability ("how to fetch the SPX chain"); context is knowledge (that a chain was
already fetched and sits in `chain_1`, and that everything must be as of a given date). Conflating
them is why multi-tool agents re-fetch everything every turn and leak look-ahead bias.

### Backbone — `core/agent.py`

One `SophieAgent` class, deliberately thin: `create_agent` accepts `system_prompt`, `middleware`,
`response_format`, `context_schema`, `checkpointer`, `store` and `cache`, so almost everything this
class used to implement by hand is now configuration passed to it.

| Was hand-rolled | Now |
|---|---|
| manual `SystemMessage` prepend each turn | `@dynamic_prompt` middleware (the prompt genuinely varies per turn — store listing + as_of) |
| `self.chat_history` list, rebuilt by the server | `checkpointer=InMemorySaver()` + `thread_id`; also retains `ToolMessage`s, which the list dropped |
| `structured()` running the loop then a **2nd** LLM call to repackage | `response_format=ToolStrategy(...)` — one graph, with validation retry |
| `max_iterations` (accepted, then silently ignored) | `ModelCallLimitMiddleware(run_limit=..., exit_behavior="end")` |
| nothing — context grew unbounded | `ContextEditingMiddleware([ClearToolUsesEdit(...)])` |
| a fallback-model *hint* inside an error string | `ModelFallbackMiddleware(...)`, opt-in per profile via `AgentProfile.fallback_models` |
| `_extract_steps()` tracking a parallel trajectory | `tool_trajectory(messages)` derives it — the messages already *are* the trajectory |
| 6-branch provider factory | `core/models.py::build_chat_model` → `init_chat_model` |

Tools declare their arguments with `Annotated[T, Field(...)]` on the signature, **not** a separate
`args_schema=` model. This is required, not cosmetic: passing an explicit `args_schema` makes
LangChain treat it as the verbatim invocation contract and skip `ToolRuntime` injection, so the tool
dies at call time with `TypeError: missing 1 required positional argument: 'runtime'`. The inferred
schema is identical — `minimum`/`maximum`/`exclusiveMinimum`/`enum`/descriptions all survive — and
being a single declaration it cannot drift from the function that consumes it (it already had:
`delegate_parallel` declared `list[ParallelTaskItem]` while its signature said
`list[dict | ParallelTaskItem]`, which is why it carried an `isinstance` ladder handling three shapes).

`chat()` returns prose, `structured()` a validated pydantic answer, `stream()` yields
`astream_events` v2 events. **v2 is correct, not stale**: v3 exists but its own docstring marks it
beta, and `ag_ui_mapper.py` is built on measured v2 shapes.

### Provider matrix — local models are first-class

Tool-calling support is **per-model, not per-provider**. Verified against the running Ollama daemon
(`qwen3.5`, `gemma4:12b`, `deepseek-r1:14b`, `bge-m3` pulled) and the DeepSeek API:

| Provider / model | Tool calling |
|---|---|
| Anthropic / OpenAI / Groq | Yes |
| DeepSeek API — `deepseek-chat` | Yes — the cheap remote tier |
| DeepSeek API — `deepseek-reasoner` (R1) | No — reasoning models don't expose function calling |
| Ollama `qwen3.5:latest` | Yes — best local option |
| Ollama `gemma4:12b` | Yes — verified with `run.py --check-models`; Gemma 4's chat template added function calling (Gemma 3 does not have it) |
| Ollama `deepseek-r1:14b` | No — same limitation as the remote R1 (confirmed live: returns prose, no tool_calls) |

`LLMModel.supports_tool_calling()` gates construction with a named error instead of a silent
empty-tool-call loop. `run.py --check-models` probes what's actually pulled locally rather than
trusting a static catalog — this caught a real gap during development: the model was initially
assumed (by family reputation) not to support tools, but the live probe against the actual pulled
build showed it does. The catalog and gate were corrected from that measurement, which is exactly
the point of probing instead of assuming.

**Current default: DeepSeek, not Anthropic.** The `ANTHROPIC_API_KEY` in this repo's `.env` returns
401 on every call — a credentials issue, not a code issue, discovered live during Phase 2 testing.
`config.py`'s `default_model_name`/`default_provider` and every profile that used to default to
`claude-sonnet-5` (`option_strategist`, `quant`, `supervisor`) now default to `deepseek-chat`
instead. Flip `SOPHIE_AGENT_MODEL` / `SOPHIE_AGENT_PROVIDER` (or the profile fields directly) back
to Anthropic once the key is fixed — nothing else needs to change.

### Delegation — `profiles.py`, `runtime.py`, `toolkits/delegate.py`

An `AgentProfile` is a subset of toolkits + a model + a role prompt + an answer type, registered in
`AGENT_PROFILES` following the `ANALYST_CONFIG` idiom already used in `src/utils/analysts.py` — adding
an agent is a data edit. Shipping profiles: `wiki_researcher` (local, free), `option_strategist`,
`quant`, `market_analyst` (DeepSeek API), `generalist`, and `supervisor` (`can_delegate=True`).

`AgentRuntime` builds agents from profiles and caches both them and their toolkits, supplying a
`SophieContext` per invocation instead of per construction. `DelegationToolkit` exposes `list_agents`,
`delegate`, and `delegate_parallel`. Guards: depth limit, no delegating to another `can_delegate`
profile, bounded concurrency, a token budget checked before each spawn, and only final answers —
never raw tool traces — flow back into the supervisor's context.

Parallel fan-out no longer needs a fresh executor per task. Specialists are shared cached
`CompiledStateGraph`s, which is safe because a LangGraph agent holds no per-run state: each task gets
its own child `SophieContext` (own `RunContext`, shared usage accumulator) and its own `thread_id`, and
the `DataFrameStore` they all write into is lock-guarded. `delegate_parallel` uses
`ThreadPoolExecutor.map`, which preserves submission order — deleting the `as_completed` +
index-bookkeeping version, and with it the `isinstance` ladder that coped with `tasks` arriving as
`ParallelTaskItem` | `dict` | bare `str`. With one schema declaration it is always a `ParallelTask`.

Hand-rolling delegation remains correct here, checked rather than assumed: `langgraph-supervisor`,
`langgraph-swarm` and `deepagents` are not installed, and langchain 1.3.15 ships no subagent
middleware. `CompiledStateGraph.as_tool()` does exist, but would give up the shared `DataFrameStore`
and the shared cross-tree usage accumulator, which are the point of this design. The persona-card UX
also depends on the exact `delegate`/`delegate_parallel` tool-call shapes.

### Toolkits — `toolkits/`

Five `SophieToolkit(BaseToolkit)` subclasses, each contributing a `system_prompt_fragment()`:

- **Wiki** — BM25-lite pure-Python retrieval over the 240 markdown files + the TS registry sidecar
  (for `summary`, the one field not in frontmatter). No vector store, no new dependency.
- **OptionChain** — live via `src/tools/api_cboe.py` (Cboe delayed quotes, IV + greeks) and
  historical via the OptionsDX parquet chains (delta only, no IV/OI — reimplemented loader, never
  imports `sophie-option-research` directly since it runs pandas 3.0.3 in its own venv).
- **Strategy** — the headline toolkit. A faithful Python port of `presets.ts` / `payoff.ts` /
  `analytics.ts` / `liquidity.ts`, keeping all 20 preset ids identical to the client so a resolved
  position is already a valid Options Viewer payload. `strategy_backtest_evidence` pulls
  `option_research_run` so a recommendation cites Sharpe/max-DD/win-rate, not folklore.
- **DataFrame** — list/schema/head plus a full Python REPL (`ast`-based, `PythonAstREPLTool`-style)
  over every DataFrame the session has produced. Executes arbitrary Python in-process by design —
  gated by `SOPHIE_AGENT_ALLOW_PYTHON`, documented plainly, not pretended-safe.
- **MarketData** — read-only guarded SQL against the market-data Postgres (curated table list,
  `SELECT`/`WITH` only, `READ ONLY` transaction, auto-`LIMIT`, real `as_of` clamping on `biz_date`
  tables) plus GraphQL against the prod Apollo endpoint.

  **The `as_of` clamp used to be a no-op and silently permitted look-ahead bias.** The injected
  wrapper was `SELECT * FROM (<query>) AS _as_of_wrapped WHERE TRUE` — `WHERE TRUE` filters nothing,
  while the tool docstring, the toolkit prompt fragment, and `runcontext.py`'s own docstring all
  promised clamping. Every `--as-of` run touching `prices`/`technicals`/`option_research_run`/etc.
  could read rows dated after `as_of`. It now enforces one of three outcomes, never a silent pass:
  the query carries its own `biz_date <= DATE '...'` bound that is **verified** to be no later than
  `as_of`; or it gets wrapped in a real `WHERE _pit_guarded.biz_date <= DATE '<as_of>'` filter; or —
  for a bound that reaches past `as_of` — it is **rejected** with an explanatory message. An
  aggregate projects no `biz_date` for the wrapper to act on, so it fails loudly and
  `_explain_sql_error` tells the model to write the bound itself. Structural checks (`;`,
  `LIMIT`) run against a literal-blanked copy of the query, with date literals deliberately preserved
  so the bound stays detectable — getting that backwards is what made the first version of this fix
  fail its own tests.

### Context — `agent_context.py`, `store.py`, `runcontext.py`

`SophieContext` (`context/agent_context.py`) bundles `run_ctx` + `store` + `config` (+ the
`AgentRuntime`, for delegation only) and is passed **per invocation** as `create_agent`'s
`context_schema`. Tools read it via a `runtime: ToolRuntime` parameter that LangGraph injects and
that never appears in the model-facing schema.

This replaced holding those three as pydantic fields on every toolkit, which cost two things:

- **Toolkits and agents had to be rebuilt for every `delegate()` call**, because each sub-agent needs
  its own `RunContext` and the `RunContext` was a constructor field. `WikiToolkit`'s module-level
  `lru_cache` existed only to stop that from re-parsing 240 markdown files per delegated task. Both
  are now cached in `AgentRuntime` and built once.
- **A pile of pydantic workarounds** to hold a lock-bearing dataclass inside a `BaseToolkit`:
  `arbitrary_types_allowed`, `SkipValidation[RunContext]`, and a
  `DelegationToolkit.model_rebuild(_types_namespace={...})` call in `runtime.py`. All deleted.

`RunContext` still carries `as_of` as injected state the model cannot widen. `DataFrameStore` remains
the spine of the tool design: bulk results (an 18k-contract chain, a thousand-row SQL result) never
enter the model's context — every bulk tool registers a handle and returns only shape + preview. One
store per `AgentRuntime`, shared across every spawned agent, thread-safe for `delegate_parallel`.
Prompt assembly now lives in the `@dynamic_prompt` middleware and runs every turn (the store listing
and as_of banner both change mid-run); `SophieAgent.preview_system_prompt()` reproduces it for
`run.py --list-tools`, and an offline test asserts the two agree.

Note this is also why `ClearToolUsesEdit` is safe here specifically: clearing older tool *text* from
the message history loses nothing, because the payload was never in the messages — it is in the store
under a handle that stays valid.

### Cross-cutting rigor

1. **Deterministic math** — the LLM never computes; every number comes from `options/payoff.py`,
   pandas, or SQL.
2. **Typed outputs** (`core/schemas.py`) — `StrategyRecommendation` with a non-empty `evidence` list
   and an `evidence_strength: backtested | conventional | unsupported` field that operationalises
   the 0.10/0.16-vs-retail-default distinction. Constraints are declared as `Field(min_length=1)` /
   `Field(ge=0, le=1)` rather than `field_validator` hooks, because `response_format` binds these
   models as an output tool — anything expressible in the JSON schema is part of the contract the
   model *sees*, whereas a validator is invisible until after it has already answered.
3. **Eval harness** (`test/agent_evals/cases.yaml`) — ~30 golden questions asserting on
   `intermediate_steps` (tool trajectory), not just final text.
4. **Tracing + cost** — LangSmith via env only, off by default; a custom `UsageCallbackHandler`
   since `get_openai_callback` is OpenAI-only. Usage is also reported to the UI on AG-UI's
   `RunFinished`/`RunError` `usage` field (see Phase 2).
5. **Caching** — `temperature=0`; a hand-rolled `SqliteCache(BaseCache)` (still justified:
   `langchain-core` ships only `InMemoryCache`, and neither `langchain-community` nor
   `langchain-classic` is installed) plus a tool-level disk cache keyed by `(tool, args, as_of)`.
   Disable with `SOPHIE_AGENT_LLM_CACHE=0` — **required for tests**, since the sqlite cache persists
   across processes and replays a cached reply without invoking the model at all, which silently made
   call-count assertions depend on what earlier runs left behind.
6. **Streaming** — `astream_events(version="v2")`.
7. **Run records** — `runs/<run_id>.json`, mirroring the `config_hash` reproducibility culture already
   in `sophie-option-research`. Written for streamed runs too; previously only `invoke()` wrote them,
   so the chat widget — which is 100% `stream()` — produced none at all.

## Extension seam

Adding a new quant use case is five mechanical steps: a `SophieToolkit` subclass, a registry entry, an
`AgentProfile`, eval cases, optionally a typed answer model. No core changes. Candidates already
sized for this: macro/regime (`fredapi` is already a dependency), cross-sectional screening,
portfolio construction/sizing, risk analytics, agentic backtesting, event studies, vol surface/skew,
research-memo drafting. **Hard rule for any future backtesting toolkit:** the agent only ever sees
in-sample metrics; out-of-sample is revealed once, by the eval harness, never requestable by the
model — an unconstrained parameter search overfits with great enthusiasm.

## Phase 2 — AG-UI API + chatbot widget

Gives the agent an HTTP surface and a chat widget in the Sophie client, so it's usable from a
browser instead of only the CLI. **The agent API is local-only, full stop — never deployed.** No
Next.js proxy route exists; the browser talks straight to `localhost:8000` over CORS. Auth, tier
gating, and page-context awareness are explicitly out of scope here — see "Out of scope" below.

```
Browser (localhost:3000)                          Python (localhost:8000)
┌──────────────────────────┐      SSE over CORS    ┌──────────────────────┐
│ ChatWidget                │ ────────────────────► │ FastAPI AG-UI        │
│ assistant-ui Thread       │ ◄──────────────────── │ SophieAgent          │
│ useAgUiRuntime            │                       │ all toolkits         │
│ HttpAgent(localhost:8000) │                       │ bound to 127.0.0.1   │
└──────────────────────────┘                       └──────────────────────┘
   dev-only, tree-shaken from prod builds             never deployed
```

### Server — `sophie_agent/server/server.py` + `sophie_agent/serve.py`

FastAPI + `ag-ui-protocol` (`ag_ui.core` types, `ag_ui.encoder.EventEncoder`). Three endpoints:
`GET /health`, `GET /agents` (the persona registry for delegation cards — see
"Persona-per-delegation" below; originally speced as "profile list for the widget's picker" before
that picker was removed), `POST /agent/{profile}` →
`StreamingResponse` of SSE-encoded AG-UI events. Binds `127.0.0.1` only, CORS restricted to
`localhost:3000`. `serve.py` never passes `host="0.0.0.0"`.

A `thread_id -> AgentRuntime` LRU registry (cap 50) keeps `DataFrameStore` alive across turns within
a thread — a chain pulled in turn 1 is still queryable in turn 3.

**History is now the checkpointer's, keyed by the same AG-UI `thread_id`.** Only the newest message is
passed in; `_convert_history` is used *only* to seed a thread the agent has no checkpoint for, which
happens when the server restarted mid-conversation while the client still holds the transcript
(`agent.has_history(thread_id)` gates it). This is strictly better than rebuilding from the client
every turn, because AG-UI's message list carries no `ToolMessage`s — verified live: turn 1 called
`wiki_search` and cited `option-strategy/gex`; turn 2 answered "what path did you cite?" correctly
with **zero** tool calls, straight from retained state.

`RunFinished`/`RunError` now also carry AG-UI's `usage: [TokenUsage]`, populated from
`RunContext.usage`. Those counts were already being accumulated and then discarded, so the UI had no
way to show run cost. (Note the count is cumulative for the thread's runtime, not per-turn.)

### Event mapper — `ag_ui_mapper.py`

Maps `SophieAgent.stream()`'s `astream_events(version="v2")` output to AG-UI events. Built from a
**measured** event trace (captured against a live Ollama run), not an assumed shape — critically,
tool-call args arrive **whole** at `on_chat_model_end`, never incrementally via `tool_call_chunks`
during `on_chat_model_stream`, on every provider tested. So the mapper drives tool-call events off
`on_tool_start`/`on_tool_end` (always fully-parsed) rather than reconstructing streamed JSON deltas
— incidentally making it provider-agnostic, since it doesn't matter whether a given provider streams
tool-call deltas or not. Text streaming drives off `on_chat_model_stream` chunks with non-empty
`.content`, bracketed by `TextMessageStart`/`End` per LLM-call `run_id` — correctly producing zero
text events for an LLM call that turns out to be a tool call. Pure function of
`(agent, message) -> event stream`; unit-tested against recorded fixtures with no network
(`test/test_ag_ui.py`).

### Generative UI — the tool→component registry

The chat renders real React components from tool results, not just markdown. **Tool return
convention:** UI-enabled tools return a JSON envelope, `{"text": ..., "ui": {"component": ..., ...}}`
(`toolkits/ui_envelope.py::ui_envelope()`), instead of a bare string. `text` is what the model reads
and reasons over; `ui` is what a matching frontend component renders.

**Which tools get `ui`, and which must not** — a context-budget decision, not a stylistic one:
`build_strategy`, `compare_strategy_variants`, `spx_gex`, `list_strategy_presets`, `wiki_search`,
`wiki_get_page` carry small, structurally-final results, so they got the envelope. **Bulk tools —
`spx_option_chain`, `spx_historical_chain`, `sql_query`, `df_python` — stay markdown+handle only,
unchanged.** An 18,000-contract chain must never enter the model's context; the `DataFrameStore`
handle pattern exists precisely to prevent this, and generative UI must not undo it.

Frontend registry: `client/src/components/chat/tool-ui/index.ts` maps tool name → component
(`StrategyLegsCard`, `WikiCitationList`, `GexChart`, `DataFrameTable` generic fallback). Wired into
`MessagePrimitive.Parts`'s `tools.by_name` map in `chat-thread.tsx` — **not**
`useAssistantToolUI`/`makeAssistantToolUI`, which are marked `@deprecated` in the installed
`@assistant-ui/react@0.15.14` and, empirically, don't wire into the AG-UI runtime adapter at all
(the hook call succeeds silently but the tool-call parts still render assistant-ui's default view;
caught via a browser-console debug pass during live E2E testing, not by reading docs).

`StrategyLegsCard` is where a Phase 1 decision paid off directly: `build_strategy` already emits
legs in the client's exact `OptionLeg` shape and keeps the same 20 preset ids, so the card feeds
`client/src/lib/options/payoff.ts` (`legsPnL`, `findBreakevens`) and charts with the
already-installed `recharts` — zero conversion, zero new dependency, a real payoff diagram.

**Gotcha worth remembering:** a tool's `result` in a `ToolCallMessagePartComponent` arrives as
**either** a raw JSON string **or** an already-parsed object — `@assistant-ui/react-ag-ui`
auto-JSON-parses tool results that look like JSON before handing them to renderers. `parseEnvelope()`
(`client/src/components/chat/tool-ui/envelope.ts`) must handle both; checking `typeof result ===
"string"` only silently drops every card with no error, which is exactly what happened before this
was caught live.

### Chat widget — `client/src/components/chat/`

`chat-widget.tsx` (floating launcher + panel) → `chat-thread.tsx` (`useAgUiRuntime` +
`AssistantRuntimeProvider`, hand-built `ThreadPrimitive`/`MessagePrimitive`/`ComposerPrimitive` UI
styled with Sophie's own design tokens — this library version ships ready-made components via the
shadcn registry as owned source, not an import, so hand-building was the intended pattern, not a
workaround) → `chat-markdown.tsx` (reuses the `remarkGfm`+`remarkMath`+`rehypeKatex` trio from
`wiki-markdown.tsx` at chat scale via `@assistant-ui/react-markdown`'s `MarkdownTextPrimitive`, but
with its own compact component map — `wiki-markdown.tsx`'s spacing is article-scale, too large for
a ~360px chat panel).
Mounted via `dev-chat-mount.tsx` in `client/src/app/layout.tsx` inside `<UserProvider>`, right after
`{children}` — the only position with Apollo + Supabase user + theme all available, and critically
**outside** `{children}` so the widget's React tree is never inside the routed page segment: a
same-tab client-side navigation to any other route re-renders `{children}` but does not unmount
`ChatWidget`, so an in-progress conversation survives clicking around the site.

#### `chat-widget.tsx` — floating launcher + panel

A `"use client"` component holding all its own UI chrome state (`open`, `position`, `size`,
`isMaximized`, `prevBounds`) in `useState` — nothing persisted, so a hard page reload always starts
closed/blank. Three independent interaction handlers built on raw Pointer Events (no drag library):
free-drag via the header bar (`handleDragStart`, skips drag if the pointerdown target is inside a
`button, input, select, a`), 8-direction resize handles (`handleResizeStart`, min 340×420), and
maximize/restore (`toggleMaximize`, remembers `prevBounds` to restore exact prior geometry). A
window-resize listener re-clamps `position` back into the viewport so the panel can't be dragged
off-screen and then stranded after a browser resize. Reset button (`initPosition`) snaps back to the
default bottom-right placement/size. Local-only enforcement layer 3 lives here too:
`allowed` starts `false` and only flips true in a `useEffect` after checking
`window.location.hostname` against `["localhost", "127.0.0.1"]` — `if (!allowed) return null` — see
"Localhost-only enforcement" above for the other two layers.

**Pop-out window** (`handlePopout`): `window.open("/agent-popout", ...)` into a real second OS-level
browser window (not a tab, not an iframe — sized/positioned via `window.screen.availWidth/Height`),
then closes the embedded panel (`setOpen(false)`). `agent-popout/page.tsx` is a standalone route
(dynamically importing `ChatThread` with `ssr: false`, same localhost-hostname gate) that renders
just the thread with a minimal title bar — no drag/resize chrome, since it's a real OS window now.
Its docstring in the header button (`title="...immune to page reloads"`) is the load-bearing fact:
because it is a genuinely separate `window.open()` window rather than something living inside the
site's own document, navigating the *main* tab around (including full reloads) cannot touch it. This
is the mechanism to reach for whenever a conversation needs to survive something the embedded
widget's same-tab-navigation mitigation (below) doesn't cover — e.g. an actual hard refresh (F5), or
navigating to an external site and back.

**`DevChatMount` gate** (`dev-chat-mount.tsx`): also hides the widget entirely on `/agent-popout`
routes (`pathname === "/agent-popout"`), so the popout window never recursively spawns another
floating launcher inside itself.

#### `chat-thread.tsx` — runtime wiring + message rendering

Shared by both the embedded widget and the popout window — both now render `<ChatThread />` with
no props at all; there is no profile prop to thread through anymore (see "Persona-per-delegation"
below for why). Per render:

- `agentUrl` (`useMemo` on `[selectedModel, selectedProvider]`) builds
  `${AGENT_API_URL}/agent/${SOPHIE_PROFILE}` (`SOPHIE_PROFILE` is the module-level constant
  `"supervisor"`) with optional `?model=&provider=` query params — `AGENT_API_URL` defaults to
  `http://localhost:8000` via `NEXT_PUBLIC_AGENT_API_URL`.
- `agent = new HttpAgent({ url: agentUrl })` and `runtime = useAgUiRuntime({ agent })`, both
  `useMemo`/hook-derived from `agentUrl` — **switching the model/provider selector constructs a
  brand-new `HttpAgent` and a brand-new runtime**, which resets the visible conversation. This is a
  second, independent way conversation state can reset besides the navigation issue below —
  expected here (the AG-UI thread is genuinely tied to one backend model), but worth knowing when
  someone reports "the chat cleared" and the actual cause was a model-selector click, not a page
  navigation. (Before the persona-per-delegation redesign, changing the profile chip did this too;
  that axis is gone now since the profile is always `supervisor`.)
- `ChatConfigContext` (verbose flag, selected model/provider) is a separate context from the AG-UI
  runtime, threaded down so `ToolInspector`/`ModelSelector` can read/set it without re-rendering the
  whole thread. `selectedModel`/`selectedProvider`/`verbose` are the **only** pieces of chat UI state
  that persist across a hard reload, via `localStorage` (`sophie_agent_model`,
  `sophie_agent_provider`, `sophie_agent_verbose`) — the message history itself is not persisted
  anywhere client-side; it lives only in the AG-UI runtime's in-memory state for the life of the
  mounted component.
- `ThreadPrimitive.Messages` maps `AssistantMessage` parts through
  `{ Text: ChatMarkdown, tools: { by_name: TOOL_BY_NAME, Fallback: TOOL_FALLBACK } }` — `TOOL_BY_NAME`
  is built once at module scope from `tool-ui/index.ts`'s `TOOL_UI` registry, each entry wrapped by
  `wrapToolUi()` which parses the tool result through `parseEnvelope()` and renders the matched React
  component plus (if verbose) a `ToolInspector` raw-trace panel; tools with no envelope or no
  registry match fall through to `TOOL_FALLBACK`, i.e. `ToolInspector` alone.

#### Gotcha — same-tab vs. new-tab links inside the chat, and why it matters for state

Any link the chat renders is either (a) a Next `<Link>` in the **same tab** — a client-side route
transition that does *not* unmount `ChatWidget` (it lives outside `{children}`, see above), so the
conversation survives — or (b) `target="_blank"` — a genuinely new browsing context, which is a real
full page load with no way around it, and boots a second, independent, empty `ChatWidget`/`ChatThread`
instance in that new tab. If the browser auto-focuses new tabs (the common default), clicking such a
link reads exactly like "the page refreshed and the agent reset," even though the original tab's
conversation is untouched in the background.

The sitewide convention, set by `WikiCard`/`WikiModal` in `article-frame.tsx` and already followed by
`chat-markdown.tsx`'s inline citation links, is same-tab `Link` with no `target`.
`tool-ui/wiki-citation-list.tsx` (the dedicated citation-card UI for `wiki_search` results)
previously diverged from that convention by adding `target="_blank"` — this was the direct cause of
users reporting the chat "refreshing" when clicking an article citation. Fixed by dropping
`target="_blank"` there so citation cards behave like every other wiki link on the site. Any new
chat tool-UI component that links to an in-app route (`/wiki/...`, `/strategy/...`, etc.) should
default to a plain same-tab `<Link>` for this reason — reach for `target="_blank"` only when the
link is genuinely external, and reach for the pop-out window (above) if a conversation specifically
needs to survive a hard reload rather than just a same-tab route change.

**Second instance of the same bug, different code path:** `chat-markdown.tsx`'s inline `a`
renderer (used for citation links the model writes as prose, not through a dedicated tool-UI card)
only treated an href as internal via `href?.startsWith("/")`. When the model cites a page using a
full absolute URL — e.g. a David Tepper article link built from a canonical URL returned by a
GraphQL/article-lookup tool result rather than a bare relative path — that check misses it and the
link falls into the `target="_blank"` branch, reintroducing the exact same new-tab/detached-agent
symptom via a different route than the citation-card one above. Fixed with `toInternalPath()`
(`chat-markdown.tsx`): resolves the href against `window.location.origin` via the `URL` constructor
and, if the origin matches, strips it down to a same-origin path for a same-tab `Link` regardless of
whether the model wrote it as `/articles/...` or `https://<site-domain>/articles/...`. Only a
genuinely different origin still falls through to `target="_blank"`. Worth remembering for *any*
future tool-UI or markdown link renderer in the chat: same-origin-ness, not "does the string start
with a slash," is the real test for "should this stay in the current tab."

#### Gotcha — `/models` response shape mismatch silently emptied the model picker

`ModelSelector` (`model-selector.tsx`) fetches `GET {AGENT_API_URL}/models` and expected the grouped,
snake_case `ModelsResponse` shape its own `FALLBACK_MODELS` uses:
`{ ollama: ModelItem[], remote: ModelItem[], default: {...} }` with `model_name`/`display_name`/
`supports_tool_calling`/`is_pulled` fields. `sophie_agent/server/server.py`'s actual `get_models()`
returns a different, flat, camelCase shape instead:
`{ models: [{ name, provider, displayName, supportsToolCalling, isLocal, pulled }], defaultModel,
defaultProvider }`. `fetchModels()` did `setModelsData(data)` with no validation, so on every
successful fetch (the common case) it overwrote the good `FALLBACK_MODELS` state with an object
whose `.ollama`/`.remote` are `undefined` — both `filteredOllama`/`filteredRemote` collapsed to `[]`,
and the dropdown showed nothing but "Profile Default (deepseek-chat)" no matter how many models the
server actually had. This is why toggling the model selector looked like "only deepseek" was ever
available. Fixed with `normalizeModelsResponse()` in `model-selector.tsx`, which maps the server's
flat array into the grouped/snake_case shape the rest of the component already expects (grouping by
`isLocal`, translating field names) before it ever reaches `setModelsData`.

While diagnosing this, two more real models-list bugs surfaced in `get_models()` and were fixed at
the same time:

- **Embedding-only Ollama models leaking into the chat picker.** Any tag pulled on the Ollama daemon
  that `get_model_info()` doesn't recognize (i.e. isn't in `AVAILABLE_MODELS`/`OLLAMA_MODELS` in
  `src/llm/models.py`) defaulted to `supportsToolCalling: True` purely because it was unrecognized —
  which is backwards for something like `bge-m3:latest` (pulled for the wiki toolkit's own use, an
  embeddings model with no chat/completion capability at all, let alone tool calling). Fixed with an
  `_is_embedding_model()` name-marker filter (`bge`, `embed`, `e5-`, `gte-`, `minilm`) that excludes
  these from the Ollama tag list before it's ever turned into picker entries.
- **Non-tool-calling models listed as if they were usable.** `deepseek-r1:14b` (Ollama) and
  `deepseek-reasoner` (DeepSeek API, `AVAILABLE_MODELS`) both correctly report
  `supports_tool_calling() == False` via `LLMModel.supports_tool_calling()` (`src/llm/models.py`) —
  but `get_models()` still listed them, just with the flag set to `false`, and the frontend rendered
  that as an amber "No tools (reasoning / chat only)" warning rather than hiding the entry. Since
  sophie_agent's entire toolkit surface reaches the model via `create_agent`'s `bind_tools` path,
  a model without tool-calling support can't drive this agent at all — selecting one wouldn't error,
  it would just silently run tool-less, prose-only turns. `get_models()` now drops any model (Ollama
  or remote) whose `supports_tool_calling()` is `False` instead of listing it as a trap option; every
  entry it returns now has `supportsToolCalling: true` unconditionally.

**Gotcha for next time:** `sophie_agent/serve.py` does not run with `--reload` by default (it's an
opt-in `--reload` CLI flag) — a `server.py`/`core/*` edit needs the process restarted
(`Stop-Process` the PID on port 8000, then `python sophie_agent/serve.py` again from the
`sophie-pipeline` root) before it takes effect, unlike the Next.js client side where Turbopack's Fast
Refresh picks up `.tsx` edits automatically without a restart.

### Persona-per-delegation — one agent (SOPHIE) in the composer, specialists surface as cards

The chat widget originally exposed all six `AGENT_PROFILES` as manually-clickable chips (Generalist,
Option Strategist, Quant, Wiki Researcher, Supervisor) — the user picked which specialist to talk to
directly, and the Delegation layer (`supervisor`/`can_delegate=True`, see above) went mostly unused
from the widget since picking "Supervisor" was just one chip among equals rather than the front
door. Redesigned so the chat only ever talks to one agent, **SOPHIE** (`supervisor`'s
`display_name` was renamed from "Supervisor" to `"Sophie"` in `core/profiles.py`), who plans and
calls `delegate()`/`delegate_parallel()` herself; when she hands work to a specialist, that
specialist appears in the thread as its own persona card rather than the user ever needing to
choose an agent up front. This is additive to the existing delegation mechanics documented above
(`profiles.py`, `runtime.py`, `toolkits/delegate/toolkit.py`) — no change to `delegate()` itself,
only to how the frontend renders its calls and which profile the widget targets.

**Backend — persona metadata + registry endpoint.** `AgentProfile` (`core/profiles.py`) gained a
`persona_icon: str = "🤖"` field (a single emoji, purely cosmetic, never read by any
agent/toolkit code) — set per specialist: `wiki_researcher` 📚, `option_strategist` 📈, `quant` 🧮,
`market_analyst` 🌐, `generalist` 🤖 (the default), `supervisor` ✨. `server.py` gained
`GET /agents`, returning `{agents: [{key, displayName, description, icon}, ...], supervisor:
{key, displayName, icon}}` — `agents` excludes `can_delegate` profiles via the same filter
`DelegationToolkit.list_agents()` already uses, so the two can never drift apart. This is the single
source of truth the frontend persona cards read from; adding a new specialist profile later needs no
frontend code change to get a reasonable persona (see the fallback behavior below) — only a
`persona_icon` for a custom one.

**Frontend — SOPHIE is hardcoded, chips are gone.** `chat-thread.tsx`'s `ChatThread` no longer takes
`profile`/`onProfileChange` props (removed from both `chat-widget.tsx` and
`app/agent-popout/page.tsx`, which now render bare `<ChatThread />`) — a module-level
`SOPHIE_PROFILE = "supervisor"` constant is used everywhere the old `profile` prop was. The controls
bar's profile-chip row was replaced with a static "✨ Sophie" label (from `sophiePersona` state,
populated by the `/agents` fetch below, falling back to a hardcoded `{displayName: "Sophie", icon:
"✨"}` while that fetch is in flight or if the server is unreachable). The model selector is
unchanged in behavior — it still overrides SOPHIE's own model via `?model=&provider=` — but its
`profileDefaultModel`/`profileDefaultProvider` props are now the module constants
`SOPHIE_DEFAULT_MODEL`/`SOPHIE_DEFAULT_PROVIDER` (`"deepseek-chat"`/`"DeepSeek"`, `supervisor`'s
configured defaults) instead of being looked up from the old `PROFILES` array, which was deleted.

`ChatThread` fetches `GET {AGENT_API_URL}/agents` once on mount into a `personas: Record<string,
AgentPersona>` map (keyed by profile key) plus `sophiePersona`, both threaded through
`ChatConfigContext` (the same context that already carried `verbose`/`selectedModel` etc.) so the
persona cards — rendered deep inside `MessagePrimitive.Parts`, several component layers away from
`ChatThread` — can read them via `useContext`.

**`tool-ui/delegate-persona-card.tsx` — the persona cards.** `delegate` and `delegate_parallel` are
registered directly in `chat-thread.tsx`'s `TOOL_BY_NAME` map, *not* routed through
`tool-ui/index.ts`'s `TOOL_UI` + `wrapToolUi()` path every other custom card uses — those two tools
return plain text (a specialist's final answer string, or for `delegate_parallel`, several joined by
blank lines), never the `{text, ui}` envelope `wrapToolUi()`/`parseEnvelope()` expect, so wrapping
them in that path would just fall through to `ToolInspector` every time.

- `DelegatePersonaCard` (`delegate`): reads `args.agent` (falling back to parsing `argsText` if
  `args` hasn't arrived yet — the tool-call args stream in before the result, per
  `ag_ui_mapper.py`'s `on_tool_start`/`on_tool_end` split) and looks it up via `personaFor()`
  against `ChatConfigContext.personas`. Renders a collapsible card: icon + specialist display name +
  a truncated one-line preview of the delegated `task`, a spinner while `status.type === "running"`,
  and the specialist's answer (through a small standalone `MiniMarkdown` — a trimmed `ReactMarkdown`
  instance, *not* `ChatMarkdown`/`MarkdownTextPrimitive`, since the latter is bound to a message's
  own streamed text part and can't render an arbitrary string like a tool result) once the result
  arrives.
- `DelegateParallelPersonaCard` (`delegate_parallel`): args are `{tasks: [{agent, task, context},
  ...]}`. The header shows every involved persona's icon in an overlapping stack plus a "N
  specialists working together" label. The result string is `DelegationToolkit.delegate_parallel`'s
  own join format — `f"[{agent}] {answer}"` blocks separated by `"\n\n"` — split back apart by
  `splitParallelResult()` (regex `^\[([^\]]+)\]\s*([\s\S]*)$` per block) so each specialist's answer
  renders under its own sub-heading rather than one undifferentiated blob.
- `personaFor(personas, key)`: if `key` isn't in the fetched registry (a profile added server-side
  after the frontend last fetched `/agents`, or the registry fetch failed), falls back to
  title-casing the raw key (`"market_analyst"` → `"Market Analyst"`) with a generic 🤖 icon — this is
  the "going forward, we may have other agents" seam: a brand-new specialist profile is usable and
  renders sensibly from the moment it exists in `AGENT_PROFILES`, with zero frontend changes required
  (a `persona_icon` is a nice-to-have, not a prerequisite).

**Verified live**, not just read from the code: a real POST to `/agent/supervisor` asking an
iron-condor-delta question produced both a `delegate_parallel` call (fanning out to
`option_strategist`/`market_analyst`/`wiki_researcher` simultaneously) and two follow-up `delegate`
calls (to `generalist` and back to `option_strategist`) in the same run — confirming SOPHIE actually
delegates rather than just being wired to in theory. Captured the raw AG-UI SSE stream and checked
the exact shapes the two persona cards depend on: `TOOL_CALL_ARGS` for the `delegate_parallel` call
carried `{"tasks": [{"agent": "option_strategist", "task": "..."}, ...]}` exactly as
`DelegateParallelPersonaCard` expects, its `TOOL_CALL_RESULT` content came back as
`"[option_strategist] I now have all the data..."` exactly matching `splitParallelResult()`'s regex,
and a plain `delegate` call's `TOOL_CALL_RESULT` was bare unprefixed text (no `[agent]` marker) as
`DelegatePersonaCard` assumes.

**Library choice:** `@assistant-ui/react` (1.45M npm downloads/wk) over CopilotKit (335K) — most
popular AG-UI-native React client, and its direct-client-connection path in CopilotKit is still
flagged `agents__unsafe_dev_only`. `@ag-ui/client` pinned to `0.0.57` (not the newer `0.0.58`) to
match what `@assistant-ui/react-ag-ui@0.0.54` itself depends on — installing "latest" independently
created two resolved copies of the package and a real `HttpAgent`-is-not-`AbstractAgent` type error
from the duplicate.

### Localhost-only enforcement — three independent layers, verified not just asserted

No proxy route exists to gate the way a hosted design would — the strongest guarantee is structural:
**the agent server only exists on your machine**, so there's nothing at any Vercel-reachable URL for
a stray request to hit regardless of what ships. Three layers on top of that:

1. **Explicit opt-in env var** — `NEXT_PUBLIC_ENABLE_CHAT=true` in the client's local `.env` only,
   never in Vercel project settings.
2. **Build-time tree-shake** — `dev-chat-mount.tsx` gates the entire `next/dynamic(() =>
   import("./chat-widget"))` call behind `process.env.NODE_ENV === "development"`. Next.js inlines
   `NODE_ENV` to a literal at build time, so in a production build the branch — and everything only
   reachable through it, including the dynamic import itself — is eliminated, not merely hidden.
   **Verified, not assumed:** `npm run build` then grepping `.next/static/` for `assistant-ui`/
   `ag-ui` found zero references to real package internals (`useAgUiRuntime`, `HttpAgent`,
   `ThreadPrimitive`, etc.) — the only string hits were unrelated pre-existing article prose that
   happens to mention "assistant-ui" as a topic.
3. **Runtime hostname guard** — `chat-widget.tsx` also checks
   `["localhost","127.0.0.1"].includes(window.location.hostname)` before mounting.

`npm run build && npm start`, driven with a real headless-Chromium session (Playwright): the
launcher button count was 0 and zero network requests to port 8000 were ever attempted — the
end-to-end proof, not just a code-reading argument.

### Live-verified, not just unit-tested

Both custom components confirmed rendering with real data via a full browser session (dev server +
local AG-UI server + local wiki/Cboe data), zero console errors: `WikiCitationList` (5 citation
cards, titles + summaries, for a live `wiki_search` call) and `StrategyLegsCard` (a real iron-condor
payoff chart, `backtested` badge, legs table — numbers matching a direct Python tool-call test
exactly: net credit 25.95, max loss -179.05). `test/test_sophie_agent.py` + `test/test_ag_ui.py`:
50/50 passing after the tool-envelope changes (a real regression check, since those tools' return
values changed shape).

## Phasing

**Phase 1:** backbone, delegation with the six shipping profiles, all five toolkits, the context
layer, all seven rigor items. CLI only.

**Phase 2:** the AG-UI server, event mapper, generative-UI tool envelope + registry, and the chat
widget, all described above. Local-only by design.

**Phase 3 (deferred, seams placed, nothing built):** auth + tier gating (needs a proxy route to
enforce server-side, which deliberately doesn't exist yet), option-viewer integration (push the legs
a user is looking at into context — cheap because `OptionLeg` shape and preset ids already match),
persistent context across sessions, long-term memory, vector retrieval behind the `wiki_search` seam,
a hosted deployment of the agent (Cloud Run, matching `services/spx-options-api/`'s pattern, with the
wiki loaded over HTTP instead of the filesystem and the historical-parquet toolkit disabled), the
additional quant toolkits from Phase 1's extension seam.

## Files

```
sophie_agent/
  __init__.py  run.py  serve.py  eval.py
  docs/        SOPHIE_AGENT.md
  core/        __init__.py  agent.py  runtime.py  profiles.py  config.py  schemas.py  callbacks.py
               models.py            <- build_chat_model() via init_chat_model + the tool-calling gate
  context/     __init__.py  runcontext.py  store.py  wiki_store.py  cache.py  run_record.py
               agent_context.py     <- SophieContext, create_agent's context_schema
  server/      __init__.py  server.py  ag_ui_mapper.py
  cli/         __init__.py  cli.py
  options/     __init__.py  presets.py  payoff.py  liquidity.py  historical.py  chain_types.py
               blackscholes.py  analytics.py
  toolkits/    __init__.py  base.py  ui_envelope.py
               wiki/       __init__.py  toolkit.py
               options/    __init__.py  toolkit.py
               strategy/   __init__.py  toolkit.py
               dataframe/  __init__.py  toolkit.py
               market/     __init__.py  toolkit.py
               delegate/   __init__.py  toolkit.py
test/          conftest.py  test_sophie_agent.py  test_ag_ui.py  test_tool_schemas.py
test/agent_evals/  cases.yaml
```

There are no per-toolkit `schemas.py` modules: tool argument schemas are inferred from
`Annotated[...]` signatures (see the Backbone section for why an explicit `args_schema` is
incompatible with `ToolRuntime` injection). `test_tool_schemas.py` asserts on the provider-facing
schema via `convert_to_openai_tool()` — note `tool.args_schema.model_json_schema()` raises, because
the inferred model still carries the injected `runtime` field whose callable members have no JSON
Schema representation.

```
ai-stock-suggestion-client/src/components/chat/
  chat-widget.tsx  chat-thread.tsx  chat-markdown.tsx  dev-chat-mount.tsx  model-selector.tsx
  tool-ui/  index.ts  envelope.ts  tool-inspector.tsx  strategy-legs-card.tsx  wiki-citation-list.tsx
            gex-chart.tsx  dataframe-table.tsx  delegate-persona-card.tsx
```

Reused, not reimplemented: `src/tools/api_db.py::get_db_connection`,
`src/tools/api_cboe.py::{get_spx_metadata, get_spx_option_chain, calculate_spx_gex}`,
`src/llm/models.py::{ModelProvider, get_model_info}` (for the empirical `supports_tool_calling()`
gate — chat-model *construction* goes through `init_chat_model`, not `get_model`; `get_model` is left
untouched because the 15 analyst agents depend on its exact Ollama sampling defaults),
`ai-stock-suggestion-client/src/lib/options/payoff.ts::{legsPnL, findBreakevens}`,
`ai-stock-suggestion-client/src/components/wiki/wiki-markdown.tsx` (plugin trio + component map),
`ai-stock-suggestion-client/src/components/ui/sticky-podcast-player.tsx` (widget geometry).

Read as reference, never imported: `sophie-option-research/src/lab/backtest.py::load_chains` (own
venv, pandas 3.0.3), `ai-stock-suggestion-client/src/lib/options/{presets,payoff,analytics,liquidity,chain-types}.ts`,
`ai-stock-suggestion-client/src/lib/wiki.ts`, `ai-stock-suggestion-client/src/lib/black-scholes.ts`.

## Out of scope (deliberately)

Everything in Phase 3 above; no GraphQL/frontend surface for the agent beyond the AG-UI endpoints
themselves; no vector store; no LangGraph rewrite; no writes to any database; no changes to the
existing 15 analyst agents or their fan-out graph; **no hosted/deployed agent of any kind** — this is
a design constraint of Phase 2, not a "for now."
