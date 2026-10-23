// Local picker-only publication. Never edit native entries or sync agent files.
import { readFileSync, writeFileSync, renameSync } from "node:fs";
import { buildMergedCatalog } from "./catalog.mjs";
import { selectedConfiguredListedModels } from "./provider-selection.mjs";
import { NATIVE_CATALOG_PATH, MERGED_CATALOG_PATH } from "./paths.mjs";
import { setModelsVisible } from "./model-picker-state.mjs";

const native = JSON.parse(readFileSync(NATIVE_CATALOG_PATH, "utf8"));
if (!native.models?.length) throw new Error("Native catalog is empty");
const external = selectedConfiguredListedModels();
const generated = buildMergedCatalog(native, external);
const nativeIds = new Set(native.models.map(model => model.slug));
const models = [
  ...native.models,
  ...generated.filter(model => !nativeIds.has(model.slug)).map(model => ({ ...model, visibility: "list" })),
];
setModelsVisible(external.map(model => model.slug), true);
const temporary = `${MERGED_CATALOG_PATH}.local-picker.tmp`;
writeFileSync(temporary, JSON.stringify({ models }, null, 2) + "\n", { mode: 0o600 });
renameSync(temporary, MERGED_CATALOG_PATH);
console.log(JSON.stringify({ nativePreserved: native.models.length, externalVisible: external.map(model => model.slug) }));
