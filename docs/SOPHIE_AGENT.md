# Sophie Agent — design doc

A LangChain tool-calling agent over the Sophie platform: 240 wiki pages, a 19-table market-data
Postgres, the public GraphQL API, live SPX option chains, and 14 years of historical OptionsDX
chains. Lives at `sophie_agent/`, entrypoints `sophie_agent/run.py` (CLI) and
`sophie_agent/serve.py` / `sophie-agent/serve.py` (local-only AG-UI HTTP API, Phase 2 — see below). The Sophie client repo's
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

### Backbone — `agent.py`

One `SophieAgent` class. LLM comes from `src/llm/models.py::get_model()` — reused, not
reimplemented. Loop is `create_tool_calling_agent` → `AgentExecutor` (`return_intermediate_steps=True`
so the eval harness can assert on tool trajectory, not just final text). Prompt is assembled by the
context layer, not hardcoded in the class. `chat()` returns prose, `structured()` returns a typed
pydantic answer, `stream()` uses `astream_events` v2.

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

`AgentRuntime` builds agents from profiles, caches them, and threads a `RunContext` (as_of, depth,
budget, usage) through every one. `DelegationToolkit` exposes `list_agents`, `delegate`, and
`delegate_parallel` (bounded `ThreadPoolExecutor`, fresh `AgentExecutor` per task, shared
`DataFrameStore` behind a lock). Guards: depth limit, no delegating to another `can_delegate` profile,
bounded concurrency, a token budget that aborts cleanly with partial results, and only final answers
— never raw tool traces — flow back into the supervisor's context.

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
- **MarketData** — read-only guarded SQL against the market-data Postgres (curated table list, `SELECT`/`WITH`
  only, `READ ONLY` transaction, auto-`LIMIT`, `as_of` auto-injected on `biz_date` columns) plus
  GraphQL against the prod Apollo endpoint.

### Context — `store.py`, `runcontext.py`, prompt assembly

`RunContext` carries `as_of` as injected state the model cannot widen. `DataFrameStore` is the spine
of the tool design: bulk results (an 18k-contract chain, a thousand-row SQL result) never enter the
model's context directly — every bulk tool registers a handle and returns only shape + preview.
One store per `AgentRuntime`, shared across every spawned agent, thread-safe for `delegate_parallel`.
System-prompt assembly concatenates the profile's role prompt, every attached toolkit's fragment, the
current `as_of` and what it forbids, and a live listing of store handles.

### Cross-cutting rigor

1. **Deterministic math** — the LLM never computes; every number comes from `options/payoff.py`,
   pandas, or SQL.
2. **Typed outputs** (`schemas.py`) — `StrategyRecommendation` with a required non-empty `evidence`
   list and an `evidence_strength: backtested | conventional | unsupported` field that operationalises
   the 0.10/0.16-vs-retail-default distinction.
3. **Eval harness** (`test/agent_evals/cases.yaml`) — ~30 golden questions asserting on
   `intermediate_steps` (tool trajectory), not just final text.
4. **Tracing + cost** — LangSmith via env only, off by default; a custom `UsageCallbackHandler`
   since `get_openai_callback` is OpenAI-only.
5. **Caching** — `temperature=0`; a hand-rolled `SqliteCache(BaseCache)` (no `langchain-community`
   available) plus a tool-level disk cache keyed by `(tool, args, as_of)`.
6. **Streaming** — `astream_events(version="v2")`.
7. **Run records** — `runs/<run_id>.json`, mirroring the `config_hash` reproducibility culture already
   in `sophie-option-research`.

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

### Server — `src/sophie_agent/server.py` + `sophie-agent/serve.py`

FastAPI + `ag-ui-protocol` (`ag_ui.core` types, `ag_ui.encoder.EventEncoder`). Three endpoints:
`GET /health`, `GET /agents` (profile list for the widget's picker), `POST /agent/{profile}` →
`StreamingResponse` of SSE-encoded AG-UI events. Binds `127.0.0.1` only, CORS restricted to
`localhost:3000`. `serve.py` never passes `host="0.0.0.0"`.

A `thread_id -> AgentRuntime` LRU registry (cap 50) keeps `DataFrameStore` alive across turns within
a thread — a chain pulled in turn 1 is still queryable in turn 3. Chat history is *not* tracked
server-side: AG-UI resends the full message list every run, so each request rebuilds
`agent.chat_history` from `input_data.messages[:-1]` (`_convert_history` in `server.py`) rather than
double-tracking against what the client believes it sent.

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
`wiki-markdown.tsx` at chat scale via `@assistant-ui/react-markdown`'s `MarkdownTextPrimitive`).
Mounted in `client/src/app/layout.tsx` inside `<UserProvider>`, right after `{children}` — the only
position with Apollo + Supabase user + theme all available.

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
  core/        __init__.py  agent.py  runtime.py  profiles.py  config.py  schemas.py  callbacks.py
  context/     __init__.py  runcontext.py  store.py  wiki_store.py  cache.py  run_record.py
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
test/          test_sophie_agent.py  test_ag_ui.py
test/agent_evals/  cases.yaml
```

```
ai-stock-suggestion-client/src/components/chat/
  chat-widget.tsx  chat-thread.tsx  chat-markdown.tsx  dev-chat-mount.tsx
  tool-ui/  index.ts  envelope.ts  strategy-legs-card.tsx  wiki-citation-list.tsx
            gex-chart.tsx  dataframe-table.tsx
```

Reused, not reimplemented: `src/tools/api_db.py::get_db_connection`,
`src/tools/api_cboe.py::{get_spx_metadata, get_spx_option_chain, calculate_spx_gex}`,
`src/llm/models.py::{get_model, ModelProvider}`,
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
