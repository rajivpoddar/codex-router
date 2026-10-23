import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { callerBaseUrl } from "../src/caller-auth.mjs";
import { EXACT_ROUTE_PROBE_HEADER } from "../src/exact-route-probe.mjs";
import { discoverSparkModel } from "../src/native-responses-routing.mjs";
import { userModelEntry } from "../src/user-models.mjs";
import { openPort } from "./port-pool.mjs";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const internalKey = "test-native-responses-internal-service-key";
const callerKey = "test-native-responses-caller-capability-key";
const models = ["deepseek/deepseek-flash", "spark/current", "cliproxy-kimi/k3"];
const museModels = ["meta/muse-spark-1.1", "meta/muse-spark-1.2", "meta/muse-spark-1.2-contributor", "meta/muse-spark-1.3", "meta/muse-spark-1.3-contributor"];
const textItem = (text) => ({ type: "message", id: "msg_test", role: "assistant", status: "completed", content: [{ type: "output_text", text, annotations: [] }] });
const reasonItem = { type: "reasoning", id: "rs_test", summary: [{ type: "summary_text", text: "REASONING_ONCE" }] };
function json(res, code, body) { res.writeHead(code, { "Content-Type": "application/json" }); res.end(JSON.stringify(body)); }
async function server(handler) {
  const srv = http.createServer(handler);
  await new Promise((resolve) => srv.listen(0, "127.0.0.1", resolve));
  return { srv, base: `http://127.0.0.1:${srv.address().port}` };
}
async function close(srv) { srv.closeAllConnections(); await new Promise((resolve) => srv.close(resolve)); }
async function read(req) { const chunks = []; for await (const c of req) chunks.push(c); return JSON.parse(Buffer.concat(chunks)); }
function stream(res, output) {
  res.writeHead(200, { "Content-Type": "text/event-stream" });
  let sequence = 0;
  const send = (type, body) => res.write(`event: ${type}\ndata: ${JSON.stringify({ type, sequence_number: sequence++, ...body })}\n\n`);
  send("response.created", { response: { id: "resp_test", object: "response", status: "in_progress", output: [] } });
  for (const [output_index, item] of output.entries()) {
    send("response.output_item.added", { output_index, item: { ...item, status: "in_progress", ...(item.type === "message" ? { content: [] } : {}) } });
    if (item.type === "message") {
      send("response.content_part.added", { output_index, item_id: item.id, content_index: 0, part: { type: "output_text", text: "", annotations: [] } });
      send("response.output_text.delta", { output_index, item_id: item.id, content_index: 0, delta: item.content[0].text });
      send("response.output_text.done", { output_index, item_id: item.id, content_index: 0, text: item.content[0].text });
      send("response.content_part.done", { output_index, item_id: item.id, content_index: 0, part: item.content[0] });
    }
    send("response.output_item.done", { output_index, item });
  }
  send("response.completed", { response: { id: "resp_test", object: "response", status: "completed", output, usage: { input_tokens: 10, output_tokens: 5, total_tokens: 15 } } });
  res.end();
}
function events(raw) { return raw.split("\n").filter((line) => line.startsWith("data: {")).map((line) => JSON.parse(line.slice(6))); }

async function harness(t, { sparkPool = false, sparkPoolFailOn = "generation" } = {}) {
  const dir = mkdtempSync(path.join(os.tmpdir(), "native-responses-test-"));
  const userModels = path.join(dir, "user-models.json");
  writeFileSync(userModels, JSON.stringify({ version: 1, models: [
    userModelEntry({ providerId: "deepseek", upstreamId: "deepseek-flash", requestProfile: "deepseek-thinking", priority: 1 }),
    ...["muse-spark-1.3", "muse-spark-1.3-contributor"].map((upstreamId) => userModelEntry({
      providerId: "meta", upstreamId, priority: 1,
      metadata: upstreamId.endsWith("-contributor") ? { toolSchemaRecursion: "flatten" } : {},
    })),
  ] }));
  const requests = [], gatewayRequests = [], nativeRequests = [], children = [];
  let loaded = "model-a", discoveryStatus = 200, discoveryCount = 0;
  const upstream = await server(async (req, res) => {
    if (req.url.endsWith("/models")) {
      discoveryCount++;
      requests.push({ url: req.url, headers: req.headers });
      if (sparkPool && sparkPoolFailOn === "discovery" && discoveryCount === 1) {
        json(res, 401, { error: { message: "Test first discovery-key refusal" } });
        return;
      }
      const current = sparkPool ? `model-for-${req.headers.authorization.split("_").at(-1)}` : loaded;
      json(res, discoveryStatus, { data: Array.isArray(current) ? current.map((id) => ({ id })) : [{ id: current }] });
      return;
    }
    const body = await read(req);
    requests.push({ url: req.url, headers: req.headers, body });
    if (sparkPool && sparkPoolFailOn === "generation" && requests.filter((item) => item.body).length === 1) {
      json(res, 429, { error: { message: "Test first-key failover" } });
      return;
    }
    const tool = body.tools?.find((entry) => entry.name?.endsWith("echo"));
    const continuation = Array.isArray(body.input) && body.input.some((item) => item.type === "function_call_output");
    const output = tool && !continuation
      ? [reasonItem, { type: "function_call", id: "fc_test", call_id: "call_test", name: tool.name, arguments: '{"value":"TOOL_ONCE"}', status: "completed" }]
      : [reasonItem, textItem("ANSWER_ONCE")];
    if (body.stream) stream(res, output);
    else json(res, 200, { id: "resp_test", object: "response", status: "completed", output, usage: { input_tokens: 10, output_tokens: 5 } });
  });
  const gateway = await server(async (req, res) => {
    if (req.method === "GET") { json(res, 200, { ok: true }); return; }
    gatewayRequests.push(await read(req));
    json(res, 200, { id: "resp_gateway", output: [textItem("LEGACY_GATEWAY")] });
  });
  const native = await server(async (req, res) => {
    nativeRequests.push(req.headers);
    req.resume(); json(res, 200, { id: "native", output: [textItem("NATIVE")] });
  });
  const apiPort = await openPort(), routerPort = await openPort();
  const env = {
    ...process.env, MODEL_ROUTER_STATE_DIR: dir, MODEL_ROUTER_USER_MODELS: userModels,
    CODEX_ROUTER_INTERNAL_KEY: internalKey, KIMI_INTERNAL_KEY: internalKey,
    CODEX_ROUTER_CALLER_KEY: callerKey, CODEX_ROUTER_SHOW_ALL_MODELS: "1", CODEX_ROUTER_QUIET: "1",
    CODEX_ROUTER_API_PORT: String(apiPort), CODEX_ROUTER_PORT: String(routerPort),
    CODEX_ROUTER_API_BASE_URL: `http://127.0.0.1:${apiPort}/v1`,
    CODEX_ROUTER_GATEWAY_BASE_URL: `${gateway.base}/v1`,
    CODEX_ROUTER_API_HEALTH_URL: `http://127.0.0.1:${apiPort}/health`,
    CODEX_ROUTER_GATEWAY_HEALTH_URL: `${gateway.base}/health`,
    CODEX_ROUTER_OAUTH_HEALTH_URL: `${gateway.base}/health`,
    CODEX_ROUTER_GROK_OAUTH_HEALTH_URL: `${gateway.base}/health`,
    CODEX_NATIVE_BASE_URL: native.base,
    DEEPSEEK_API_BASE_URL: `${upstream.base}/deepseek`, DEEPSEEK_API_KEY: "TEST_DEEPSEEK_KEY",
    SPARK_CURRENT_BASE_URL: `${upstream.base}/spark/v1`, SPARK_QWEN_API_KEY: "TEST_SPARK_KEY",
    CLIPROXY_KIMI_BASE_URL: `${upstream.base}/kimi/v1`, CLIPROXY_API_KEY: "TEST_KIMI_KEY",
    CLIPROXY_GROK_BASE_URL: `${upstream.base}/grok/v1`, CLIPROXY_GROK_API_KEY: "TEST_GROK_KEY",
    CLIPROXY_ANTIGRAVITY_BASE_URL: `${upstream.base}/antigravity/v1`,
    CLIPROXY_ANTIGRAVITY_API_KEY: "TEST_ANTIGRAVITY_KEY",
    META_BASE_URL: `${upstream.base}/meta/v1`, META_API_KEY: "TEST_META_KEY",
  };
  t.after(async () => {
    for (const child of children) {
      if (child.exitCode === null && child.signalCode === null) {
        child.kill("SIGTERM"); await new Promise((resolve) => child.once("exit", resolve));
      }
    }
    await Promise.all([close(upstream.srv), close(gateway.srv), close(native.srv)]);
    rmSync(dir, { recursive: true, force: true });
  });
  if (sparkPool) {
    env.SPARK_QWEN_API_KEY = "TEST_POOL_ONE";
    writeFileSync(path.join(dir, "spark-qwen-api-key.secret"), "TEST_POOL_TWO", { mode: 0o600 });
    // Credential-store paths are confined to the process's router state root.
    // Build the fixture in a child with the same isolated root as the servers.
    execFileSync(process.execPath, ["--input-type=module", "-e", `
      import { addCredentialReference } from './src/provider-credential-store.mjs';
      import { upsertProviderApiKey } from './src/provider-api-key-pool.mjs';
      for (const secretRef of [{ type: 'environment', name: 'SPARK_QWEN_API_KEY' }, { type: 'provider-file' }]) {
        const entry = addCredentialReference({ providerId: 'spark', kind: 'api_key', secretRef });
        await upsertProviderApiKey('spark', { id: entry.id });
      }
    `], { cwd: root, env, stdio: "pipe" });
  }
  for (const script of ["api-forwarder.mjs", "router.mjs"]) {
    const child = spawn(process.execPath, [path.join(root, "src", script)], { env, stdio: ["ignore", "ignore", "pipe"] });
    child.errors = ""; child.stderr.on("data", (chunk) => child.errors += chunk); children.push(child);
    const url = script === "router.mjs" ? `${callerBaseUrl(routerPort, callerKey)}/models` : `http://127.0.0.1:${apiPort}/health`;
    let ready = false;
    for (let i = 0; i < 150; i++) {
      assert.equal(child.exitCode, null, child.errors);
      try { if ((await fetch(url, { headers: { Authorization: `Bearer ${internalKey}` } })).ok) { ready = true; break; } } catch {}
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    assert.ok(ready, child.errors);
  }
  return {
    requests, gatewayRequests, nativeRequests,
    setLoaded: (value) => loaded = value, setDiscoveryStatus: (value) => discoveryStatus = value,
    discoveryCount: () => discoveryCount,
    post: async (model, body = {}, suffix = "/responses") => {
      const response = await fetch(`${callerBaseUrl(routerPort, callerKey)}${suffix}`, {
        method: "POST", headers: { Authorization: "Bearer native-caller-token-must-not-leak", "Content-Type": "application/json", "ChatGPT-Account-Id": "native-account-must-not-leak", [EXACT_ROUTE_PROBE_HEADER]: "1" },
        body: JSON.stringify({ model, input: [{ role: "user", content: "Test the protocol" }], ...body }),
      });
      if (!response.ok) console.error(children.map(child => child.errors).join("\n"));
      return response;
    },
  };
}

test("three native routes bypass the gateway, preserve reasoning, continue tools, and compact", async (t) => {
  const h = await harness(t);
  for (const model of models) {
    const tools = [{ type: "function", name: "echo", parameters: { type: "object", properties: { value: { type: "string" } }, required: ["value"] } }];
    const response = await h.post(model, { stream: true, tools, reasoning: { effort: "low" }, input: [reasonItem, textItem("OLD_VISIBLE"), { role: "user", content: "Call echo" }] });
    const raw = await response.text();
    assert.equal(response.status, 200, raw);
    const generation = h.requests.at(-1);
    assert.ok(generation.url.endsWith("/responses"));
    assert.ok(generation.body.input.some((item) => item.type === "reasoning"), model);
    const visibleHistory = generation.body.input.filter((item) => item.type === "message" || item.role).map((item) => JSON.stringify(item.content)).join("");
    assert.ok(!visibleHistory.includes("REASONING_ONCE"), model);
    assert.equal(generation.body.reasoning_effort, undefined);
    assert.equal(generation.body.thinking, undefined);
    assert.equal(generation.body.reasoning?.effort, model === "spark/current" ? undefined : "low");
    assert.equal(generation.headers.authorization, `Bearer TEST_${model.startsWith("deepseek") ? "DEEPSEEK" : model.startsWith("spark") ? "SPARK" : "KIMI"}_KEY`);
    assert.equal(generation.headers["chatgpt-account-id"], undefined);
    const terminal = events(raw).find((event) => event.type === "response.completed");
    assert.ok(terminal, raw);
    assert.equal(terminal.response.output.filter((item) => item.type === "function_call").length, 1);
    const follow = await h.post(model, { stream: true, tools, input: [{ role: "user", content: "Call echo" }, ...terminal.response.output, { type: "function_call_output", call_id: "call_test", output: "TOOL_ONCE" }] });
    const followRaw = await follow.text();
    assert.equal(follow.status, 200, followRaw);
    assert.equal(events(followRaw).filter((event) => event.type === "response.output_text.delta").map((event) => event.delta).join(""), "ANSWER_ONCE");
    assert.equal(events(followRaw).find((event) => event.type === "response.completed").response.output.filter((item) => item.type === "message").length, 1);
    const compact = await h.post(model, { input: [{ role: "user", content: "Remember the tool returned TOOL_ONCE" }, textItem("Task complete") ] }, "/responses/compact");
    const compactBody = await compact.text();
    assert.equal(compact.status, 200, compactBody);
    assert.match(compactBody, /BEGIN_CODEX_ROUTER_CHECKPOINT_V2/);
  }
  assert.equal(h.gatewayRequests.length, 0);
  assert.equal(h.nativeRequests.length, 0);
  // Positive control: a non-migrated DeepSeek model still takes the gateway.
  const legacy = await h.post("deepseek/deepseek-v4-flash");
  assert.equal(legacy.status, 200, await legacy.text());
  assert.equal(h.gatewayRequests.length, 1);
  const native = await h.post("gpt-6-astra");
  assert.equal(native.status, 200, await native.text());
  assert.equal(h.nativeRequests.length, 1);
  assert.equal(h.nativeRequests[0].authorization, "Bearer native-caller-token-must-not-leak");
  assert.equal(h.nativeRequests[0]["chatgpt-account-id"], "native-account-must-not-leak");
  assert.equal(h.gatewayRequests.length, 1);
});

test("all Muse routes bypass LiteLLM while preserving Meta effort and scoped tool compatibility", async (t) => {
  const h = await harness(t);
  const recursiveSchema = {
    type: "object", properties: { node: { $ref: "#/$defs/node" } },
    $defs: { node: { type: "object", properties: { child: { $ref: "#/$defs/node" } } } },
  };
  const tools = [
    { type: "function", name: "echo", parameters: { type: "object", properties: { value: { type: "string" } } } },
    { type: "namespace", name: "workspace", tools: [{ type: "function", name: "walk", parameters: recursiveSchema }] },
    { type: "custom", name: "apply_patch", format: { type: "text" } },
    { type: "web_search", search_content_types: ["text"] },
  ];
  for (const model of museModels) {
    const response = await h.post(model, {
      stream: true, tools, reasoning: { effort: "xhigh" }, tool_choice: "required",
      input: [reasonItem, textItem("OLD_VISIBLE"), { role: "user", content: "Call echo" }],
    });
    const raw = await response.text();
    assert.equal(response.status, 200, raw);
    const sent = h.requests.at(-1);
    assert.equal(sent.url, "/meta/v1/responses");
    assert.equal(sent.headers.authorization, "Bearer TEST_META_KEY");
    assert.equal(sent.headers["chatgpt-account-id"], undefined);
    assert.equal(sent.body.model, model.slice("meta/".length));
    assert.equal(sent.body.reasoning.effort, "xhigh", "Meta must not acquire DeepSeek's max mapping");
    assert.equal(sent.body.reasoning_effort, undefined);
    assert.equal(sent.body.thinking, undefined);
    assert.equal(sent.body.tool_choice, /1\.[23]/.test(model) ? "auto" : "required");
    assert.ok(sent.body.input.some((item) => item.type === "reasoning"));
    assert.ok(!sent.body.input.filter((item) => item.type === "message").some((item) => JSON.stringify(item.content).includes("REASONING_ONCE")));
    assert.ok(!JSON.stringify(sent.body.tools).includes("search_content_types"));
    assert.ok(!sent.body.tools.some((tool) => ["namespace", "custom"].includes(tool.type)));
    assert.ok(sent.body.tools.some((tool) => tool.name === "apply_patch" && tool.type === "function"));
    const schema = sent.body.tools.find((tool) => tool.name.endsWith("walk")).parameters;
    assert.equal(schema.properties.node.$ref, "#/$defs/node", "Acyclic references stay usable");
    assert.deepEqual(schema.$defs.node.properties.child, {}, "The recursive edge must be removed");
    const terminal = events(raw).find((event) => event.type === "response.completed");
    assert.equal(terminal.response.output.filter((item) => item.type === "function_call").length, 1);
    const follow = await h.post(model, { stream: true, tools, input: [{ role: "user", content: "Call echo" }, ...terminal.response.output, { type: "function_call_output", call_id: "call_test", output: "TOOL_ONCE" }] });
    const followRaw = await follow.text();
    assert.equal(follow.status, 200, followRaw);
    assert.equal(events(followRaw).filter((event) => event.type === "response.output_text.delta").map((event) => event.delta).join(""), "ANSWER_ONCE");
    const compact = await h.post(model, { reasoning: { effort: "medium" } }, "/responses/compact");
    assert.equal(compact.status, 200, await compact.text());
    assert.equal(h.requests.at(-1).body.reasoning.effort, "medium");
    assert.equal(h.requests.at(-1).url, "/meta/v1/responses");
  }
  assert.equal(h.gatewayRequests.length, 0);
  assert.equal(h.nativeRequests.length, 0);
});

test("CLIProxyAPI-hosted Grok bypasses LiteLLM, keeps hosted search, and bridges custom tools", async (t) => {
  const h = await harness(t);
  const tools = [
    { type: "function", name: "echo", parameters: { type: "object", properties: { value: { type: "string" } }, required: ["value"] } },
    { type: "custom", name: "apply_patch", format: { type: "text" } },
    { type: "web_search", search_content_types: ["text"] },
  ];
  for (const model of ["cliproxy-grok/grok-4.6", "cliproxy-grok/grok-4.5"]) {
    const response = await h.post(model, {
      stream: true, tools, reasoning: { effort: "medium" }, tool_choice: "required",
      input: [reasonItem, textItem("OLD_VISIBLE"), { role: "user", content: "Call echo" }],
    });
    const raw = await response.text();
    assert.equal(response.status, 200, raw);
    const sent = h.requests.at(-1);
    assert.equal(sent.url, "/grok/v1/responses");
    assert.equal(sent.headers.authorization, "Bearer TEST_GROK_KEY");
    assert.equal(sent.headers["chatgpt-account-id"], undefined);
    assert.equal(sent.body.model, model.slice("cliproxy-grok/".length));
    // xAI reads effort from the nested Responses field, never the flat one.
    assert.equal(sent.body.reasoning?.effort, "medium");
    assert.equal(sent.body.reasoning_effort, undefined);
    assert.equal(sent.body.thinking, undefined);
    assert.ok(sent.body.input.some((item) => item.type === "reasoning"));
    assert.ok(!JSON.stringify(sent.body.tools).includes("search_content_types"));
    assert.ok(!sent.body.tools.some((tool) => tool.type === "custom"));
    assert.ok(sent.body.tools.some((tool) => tool.type === "function" && tool.name === "apply_patch"));
    assert.ok(sent.body.tools.some((tool) => tool.type === "web_search"));
    const terminal = events(raw).find((event) => event.type === "response.completed");
    assert.equal(terminal.response.output.filter((item) => item.type === "function_call").length, 1);
    const follow = await h.post(model, {
      stream: true, tools,
      input: [
        { role: "user", content: "Call echo" },
        ...terminal.response.output,
        { type: "function_call_output", call_id: "call_test", output: "TOOL_ONCE" },
      ],
    });
    const followRaw = await follow.text();
    assert.equal(follow.status, 200, followRaw);
    assert.equal(
      events(followRaw).filter((event) => event.type === "response.output_text.delta").map((event) => event.delta).join(""),
      "ANSWER_ONCE",
    );
  }
  assert.ok(h.requests.every((item) => !item.url.includes("/kimi/")), "Grok must not borrow the Kimi route");
  assert.equal(h.gatewayRequests.length, 0);
  assert.equal(h.nativeRequests.length, 0);
});

test("CLIProxyAPI-hosted Gemini 3.8 Flash bypasses LiteLLM, keeps recursive schemas, and bridges custom tools", async (t) => {
  const h = await harness(t);
  const recursiveSchema = {
    type: "object", properties: { node: { $ref: "#/$defs/node" } },
    $defs: { node: { type: "object", properties: { child: { $ref: "#/$defs/node" } } } },
  };
  const tools = [
    { type: "function", name: "echo", parameters: { type: "object", properties: { value: { type: "string" } }, required: ["value"] } },
    { type: "namespace", name: "workspace", tools: [{ type: "function", name: "walk", parameters: recursiveSchema }] },
    { type: "custom", name: "apply_patch", format: { type: "text" } },
  ];
  const response = await h.post("cliproxy-antigravity/gemini-3.8-flash", {
    stream: true, tools, reasoning: { effort: "medium" }, tool_choice: "required",
    input: [reasonItem, textItem("OLD_VISIBLE"), { role: "user", content: "Call echo" }],
  });
  const raw = await response.text();
  assert.equal(response.status, 200, raw);
  const sent = h.requests.at(-1);
  assert.equal(sent.url, "/antigravity/v1/responses");
  assert.equal(sent.headers.authorization, "Bearer TEST_ANTIGRAVITY_KEY");
  assert.equal(sent.headers["chatgpt-account-id"], undefined);
  // The vendor id carries the tier; Codex effort stays on the nested field the
  // CLIProxyAPI carrier reads.
  assert.equal(sent.body.model, "gemini-3.8-flash-high");
  assert.equal(sent.body.reasoning?.effort, "medium");
  assert.equal(sent.body.reasoning_effort, undefined);
  assert.equal(sent.body.thinking, undefined);
  assert.ok(sent.body.input.some((item) => item.type === "reasoning"));
  assert.ok(!sent.body.input.filter((item) => item.type === "message").some((item) => JSON.stringify(item.content).includes("REASONING_ONCE")));
  // Gemini's protobuf schema layer accepts recursive local references, so the
  // cycle must survive intact instead of being flattened the way Meta's is.
  const schema = sent.body.tools.find((tool) => tool.name.endsWith("walk")).parameters;
  assert.equal(schema.properties.node.$ref, "#/$defs/node");
  assert.deepEqual(schema.$defs.node.properties.child, { $ref: "#/$defs/node" });
  // A freeform custom tool is accepted and then silently ignored upstream, so
  // it must arrive as an ordinary function for the forced choice to work.
  assert.ok(!sent.body.tools.some((tool) => tool.type === "custom"));
  assert.ok(sent.body.tools.some((tool) => tool.type === "function" && tool.name === "apply_patch"));
  const terminal = events(raw).find((event) => event.type === "response.completed");
  assert.equal(terminal.response.output.filter((item) => item.type === "function_call").length, 1);
  const follow = await h.post("cliproxy-antigravity/gemini-3.8-flash", {
    stream: true, tools,
    input: [
      { role: "user", content: "Call echo" },
      ...terminal.response.output,
      { type: "function_call_output", call_id: "call_test", output: "TOOL_ONCE" },
    ],
  });
  const followRaw = await follow.text();
  assert.equal(follow.status, 200, followRaw);
  assert.equal(
    events(followRaw).filter((event) => event.type === "response.output_text.delta").map((event) => event.delta).join(""),
    "ANSWER_ONCE",
  );
  assert.equal(h.requests.at(-1).url, "/antigravity/v1/responses");
  const compact = await h.post("cliproxy-antigravity/gemini-3.8-flash", { input: [textItem("Task complete")] }, "/responses/compact");
  const compactBody = await compact.text();
  assert.equal(compact.status, 200, compactBody);
  assert.match(compactBody, /BEGIN_CODEX_ROUTER_CHECKPOINT_V2/);
  assert.ok(h.requests.every((item) => !item.url.includes("/kimi/")), "Gemini must not borrow the Kimi route");
  assert.equal(h.gatewayRequests.length, 0);
  assert.equal(h.nativeRequests.length, 0);
});

test("Spark discovers each loaded model without restarting and refuses failed or ambiguous discovery", async (t) => {
  const h = await harness(t);
  for (const loaded of ["ornith-a", "qwen-b"]) {
    h.setLoaded(loaded);
    const response = await h.post("spark/current");
    assert.equal(response.status, 200, await response.text());
    assert.equal(h.requests.at(-1).body.model, loaded);
  }
  assert.equal(h.discoveryCount(), 2);
  assert.ok(h.requests.filter((item) => !item.body).every((item) => item.headers.authorization === "Bearer TEST_SPARK_KEY" && item.headers["chatgpt-account-id"] === undefined));
  for (const invalid of [[], ["a", "b"]]) {
    h.setLoaded(invalid);
    const response = await h.post("spark/current");
    assert.equal(response.status, 502, await response.text());
  }
  h.setDiscoveryStatus(401);
  const refused = await h.post("spark/current");
  assert.equal(refused.status, 502, await refused.text());
  assert.equal(h.requests.filter((item) => item.body).length, 2);
  assert.equal(h.gatewayRequests.length, 0);
});

test("Spark discovery rejects redirects and bounds stalled requests without leaking keys", async () => {
  let leaked = 0;
  const sink = await server((req, res) => { leaked++; json(res, 200, { data: [{ id: "stolen" }] }); });
  const source = await server((req, res) => {
    if (req.url === "/redirect/models") { res.writeHead(307, { Location: `${sink.base}/models` }); res.end(); }
    // Other paths deliberately never respond.
  });
  try {
    const check = (error) => error.status === 502 && error.code === "spark_model_discovery_failed" && !error.message.includes("SECRET");
    await assert.rejects(discoverSparkModel({ baseUrl: `${source.base}/redirect`, apiKey: "SECRET", timeoutMs: 500 }), check);
    await assert.rejects(discoverSparkModel({ baseUrl: `${source.base}/stall`, apiKey: "SECRET", timeoutMs: 30 }), check);
    const controller = new AbortController();
    const request = discoverSparkModel({ baseUrl: source.base, apiKey: "SECRET", signal: controller.signal });
    controller.abort();
    await assert.rejects(request, check);
    assert.equal(leaked, 0);
  } finally { await close(source.srv); await close(sink.srv); }
});

test("Spark pool failover discovers and generates with the same authoritative key per attempt", async (t) => {
  const h = await harness(t, { sparkPool: true });
  const response = await h.post("spark/current");
  assert.equal(response.status, 200, await response.text());
  assert.equal(h.requests.length, 4, "Two discovery/generation pairs are required");
  for (const offset of [0, 2]) {
    const [discovery, generation] = h.requests.slice(offset, offset + 2);
    assert.ok(discovery.url.endsWith("/models"));
    assert.ok(generation.url.endsWith("/responses"));
    assert.equal(discovery.headers.authorization, generation.headers.authorization);
    assert.equal(generation.body.model, `model-for-${generation.headers.authorization.split("_").at(-1)}`);
  }
  assert.notEqual(h.requests[0].headers.authorization, h.requests[2].headers.authorization);
  assert.equal(h.gatewayRequests.length, 0);
});

test("Spark discovery-key refusal rotates before generation and uses the replacement key's model", async (t) => {
  const h = await harness(t, { sparkPool: true, sparkPoolFailOn: "discovery" });
  const response = await h.post("spark/current");
  assert.equal(response.status, 200, await response.text());
  assert.equal(h.requests.length, 3);
  const [refused, discovery, generation] = h.requests;
  assert.notEqual(refused.headers.authorization, discovery.headers.authorization);
  assert.equal(discovery.headers.authorization, generation.headers.authorization);
  assert.equal(generation.body.model, `model-for-${generation.headers.authorization.split("_").at(-1)}`);
  assert.equal(h.gatewayRequests.length, 0);
});
