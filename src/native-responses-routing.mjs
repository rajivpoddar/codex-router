import { readResponseBody } from "./http-utils.mjs";

// These upstream Responses contracts have been exercised with Codex. Keep
// other models on their existing gateway path until their contract is tested.
// This still uses the authenticated API forwarder, not the keyless local
// ChatGPT bridge's distinct `directResponses` contract.
export function usesNativeResponses(model) {
  return (
    (model?.provider === "deepseek" && model.upstreamModel === "deepseek-flash") ||
    (model?.provider === "spark" && model.requestProfile === "spark-server-default") ||
    (model?.provider === "cliproxy-kimi" && model.upstreamModel === "kimi-k3") ||
    (model?.provider === "cliproxy-grok" && model.upstreamModel?.startsWith("grok-")) ||
    (model?.provider === "cliproxy-antigravity" &&
      (model.upstreamModel?.startsWith("gemini-") ||
       model.upstreamModel?.startsWith("antigravity-claude-") ||
       model.upstreamModel?.startsWith("claude-"))) ||
    (model?.provider === "cliproxy-claude" &&
      (model.upstreamModel?.startsWith("claude-") ||
       model.upstreamModel?.startsWith("opus-") ||
       model.upstreamModel?.startsWith("fable-"))) ||
    (model?.provider === "meta" && model.upstreamModel?.startsWith("muse-spark-"))
  );
}

export async function discoverSparkModel({ baseUrl, apiKey, signal, timeoutMs = 10_000 }) {
  let providerStatus;
  const boundedSignal = AbortSignal.any([
    ...(signal ? [signal] : []),
    AbortSignal.timeout(timeoutMs),
  ]);
  try {
    const response = await fetch(`${baseUrl.replace(/\/$/, "")}/models`, {
      headers: { Authorization: `Bearer ${apiKey}`, Accept: "application/json" },
      redirect: "error",
      signal: boundedSignal,
    });
    if (!response.ok) {
      providerStatus = response.status;
      await response.body?.cancel();
      throw new Error("Model discovery was refused.");
    }
    const body = JSON.parse((await readResponseBody(response, {
      signal: boundedSignal,
      maxBytes: 1024 * 1024,
    })).toString("utf8"));
    const ids = new Set((Array.isArray(body.data) ? body.data : [])
      .map((model) => model?.id)
      .filter((id) => typeof id === "string" && id.trim()));
    if (ids.size !== 1) throw new Error("Expected one loaded model.");
    return [...ids][0];
  } catch {
    // Never include upstream bodies, URLs, or credentials in the public error.
    const error = new Error("Spark must advertise one loaded model at /v1/models.");
    error.status = 502;
    error.code = "spark_model_discovery_failed";
    // The authoritative key-pool runner can rotate a refused/rate-limited
    // key before generation; callers still receive a sanitized discovery 502.
    if (providerStatus !== undefined) error.providerStatus = providerStatus;
    throw error;
  }
}
