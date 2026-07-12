import { readFileSync } from "node:fs";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
let playwright;
try {
  playwright = require("playwright");
} catch (error) {
  const moduleDir = process.env.PLAYWRIGHT_MODULE_DIR;
  if (!moduleDir) throw error;
  playwright = require(`${moduleDir}/playwright`);
}
const { chromium } = playwright;

const inputPath = process.argv[2];
const decisionPath = process.argv[3] && process.argv[3] !== "-" ? process.argv[3] : null;
const limit = Number(process.argv[4] ?? 64);
if (!inputPath) {
  console.error("usage: node playwright_slot_smoke.mjs <final-kits.jsonl> [lfm-slot-decisions.json|-] [limit]");
  process.exit(2);
}

const requiredSlots = ["name", "domain_path", "requires", "provides", "resources", "public_api", "tests"];
const records = readFileSync(inputPath, "utf8")
  .split(/\n+/)
  .filter(Boolean)
  .slice(0, limit)
  .map((line) => JSON.parse(line));
const decisions = decisionPath ? JSON.parse(readFileSync(decisionPath, "utf8")) : null;

const html = `<!doctype html>
<meta charset="utf-8">
<title>Kit Slot Smoke</title>
<script>
window.__KIT_SLOT_SMOKE__ = {
  state: { accepted: 0, rejected: 0, checks: [] },
  reset() { this.state = { accepted: 0, rejected: 0, checks: [] }; },
  step(record) {
    const kit = record.payload || {};
    const missing = ${JSON.stringify(requiredSlots)}.filter((slot) => !kit[slot] || (Array.isArray(kit[slot]) && !kit[slot].length));
    const rb = kit.renderer_boundary || {};
    const rendererOwned = Boolean(rb.ownsDom || rb.ownsCanvas || rb.ownsThreeObjects);
    const ok = missing.length === 0 && !rendererOwned && kit.atomic === true && kit.idempotent === true;
    const check = { ok, name: kit.name, missing, rendererOwned };
    this.state.checks.push(check);
    if (ok) this.state.accepted += 1; else this.state.rejected += 1;
    return check;
  },
  replayDecisionTree(decisionReport) {
    const decisions = (decisionReport && decisionReport.decisions) || [];
    let accepted = 0;
    let rejected = 0;
    const traces = [];
    for (const decision of decisions) {
      const nodes = decision.nodes || [];
      const ok = decision.accepted === true && nodes.length > 0 && nodes.every((node) => node.verdict === "Y");
      if (ok) accepted += 1; else rejected += 1;
      traces.push({ ok, recordId: decision.record_id, nodeCount: nodes.length });
    }
    this.state.decisionReplay = { accepted, rejected, traces };
    return this.state.decisionReplay;
  },
  snapshot() { return JSON.parse(JSON.stringify(this.state)); }
};
</script>
<body>kit slot smoke</body>`;

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ javaScriptEnabled: true });
await context.route("**/*", (route) => route.abort());
const page = await context.newPage();
await page.setContent(html, { waitUntil: "domcontentloaded" });
await page.evaluate((items) => {
  window.__KIT_SLOT_SMOKE__.reset();
  for (const item of items) window.__KIT_SLOT_SMOKE__.step(item);
}, records);
if (decisions) {
  await page.evaluate((decisionReport) => {
    window.__KIT_SLOT_SMOKE__.replayDecisionTree(decisionReport);
  }, decisions);
}
const snapshot = await page.evaluate(() => window.__KIT_SLOT_SMOKE__.snapshot());
await browser.close();

const decisionReplay = snapshot.decisionReplay || { accepted: 0, rejected: 0, traces: [] };
const report = {
  ok: snapshot.accepted === records.length && snapshot.rejected === 0 && (!decisions || decisionReplay.rejected === 0),
  recordsTested: records.length,
  accepted: snapshot.accepted,
  rejected: snapshot.rejected,
  decisionReplay,
  networkPolicy: "all routes aborted; data URL/setContent only",
  writePolicy: "no artifact writes",
  sampleChecks: snapshot.checks.slice(0, 10)
};
console.log(JSON.stringify(report, null, 2));
process.exit(report.ok ? 0 : 1);
