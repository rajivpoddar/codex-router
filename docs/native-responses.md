# Native Responses routes

`deepseek/deepseek-flash`, `spark/current`, `cliproxy-kimi/k3`, the
`cliproxy-grok/grok-4.5` and `cliproxy-grok/grok-4.6` rows, the
`cliproxy-antigravity/gemini-3.8-flash` row, and the configured
`meta/muse-spark-*` models use:

Codex → Router → authenticated API forwarder → upstream `/responses`.

Normal turns and router compaction bypass LiteLLM. The API forwarder still owns
provider credentials, endpoint selection, rate-limit accounting, and parameter
normalization. Responses reasoning stays in reasoning items rather than being
copied into visible assistant messages. DeepSeek and Spark retain the existing
function-tool bridge; Kimi retains its native Responses tool format. Meta keeps
its reasoning levels. All five configured Muse variants reject custom tools
and recursive schemas, so they use the function/custom-tool bridge and
recursive-schema repair, restoring Codex's original calls and results.
xAI accepts a freeform `type: "custom"` definition on `/v1/responses` but never
returns a custom call for it -- a forced `apply_patch` choice comes back as no
tool call at all -- so the CLIProxy-hosted Grok rows take the same
function/custom-tool bridge. Grok's tool calls, hosted search, and reasoning
summaries are otherwise native, and CLIProxyAPI already owns the xAI OAuth
session, so this route needs no `grok-oauth` forwarder and no LiteLLM hop.

CLIProxyAPI's Gemini surface behaves the same way for custom tools, so
`cliproxy-antigravity/gemini-3.8-flash` takes the function/custom-tool bridge
too. It keeps recursive local JSON-Schema references intact -- unlike the Meta
routes, this endpoint accepts them -- and it publishes no search tool: a
`web_search` tool is accepted and then answered with no message at all, so
advertising hosted search would break the turn. Codex effort travels on the
nested `reasoning.effort` field, which CLIProxyAPI maps onto the Gemini
thinking budget; the vendor model id carries the `-high` tier and is fixed.
CLIProxyAPI already owns the Antigravity OAuth session, so this route needs no
`antigravity-oauth` login and no LiteLLM hop. The row is served only by
CLIProxyAPI on `127.0.0.1:8317`; the router's own Antigravity OAuth provider is
separate and still unconfigured.

Spark discovers its model on every request; see [Spark current](spark-current.md).
Other models and native OpenAI routing keep their current paths.
OpenAI API compatibility alone does not certify a
provider's support for every Codex tool or reasoning field.

After changing router code, restart the router service. These routes keep their
existing model slugs and picker rows; no picker publication or Codex restart is
needed. Removing LiteLLM from these paths eliminates its translation step, but
does not by itself prove the cause of repetition seen in older task histories.
