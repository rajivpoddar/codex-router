# Local Spark route

`spark/current` is a stable external Codex Router route named **Spark Current**.
It connects to the native Responses endpoint on port 30000 through the API
forwarder, bypassing LiteLLM. Before each Responses request, the forwarder reads
the loaded model from the same authenticated `/v1/models` endpoint. SGLang's
Responses endpoint requires that ID even though Chat Completions accepts an
omitted model. Discovery is bounded to 10 seconds and must return one distinct
model ID; a failure stops the request instead of sending a guessed model.

At a server model cutover:

1. Keep the same endpoint and bearer-key contract. The next request uses the
   server's default model; a model-name change requires no router restart.
2. Keep `upstreamModel: "server-default"` as the registry placeholder.
   `spark-server-default` replaces that placeholder with the discovered ID and
   removes the client effort override. It does not cache or guess a model ID.
   Direct Chat Completions requests still omit the model as before.
3. The picker retains a 262,144-token context window and 220,000-token
   auto-compaction threshold. Update these limits if a new server uses a
   smaller context. Image and tool capabilities are not automatically detected.
4. If the server address, authentication, context settings, or request
   contract changes, update that configuration and restart the router. A new
   server must expose `/v1/models` and `/v1/responses` for this route to work.

For picker changes, update only the `spark/current` row and preserve all other
entries. Do not use the full installer or rebuild the local picker from an old
native catalog snapshot. Native companion settings remain separate.
Codex uses the router's native pass-through for OpenAI requests; realtime
endpoints are pinned directly to OpenAI. Existing conversations are not cleared
by a route change; start a fresh task if the new context limit is smaller.

The provider reuses the existing protected Spark credential reference; no key
is stored in these files. These local registry additions must be preserved
across checkout replacement, alongside the migrated routes.
