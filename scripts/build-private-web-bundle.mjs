#!/usr/bin/env node
import { createCipheriv, pbkdf2Sync, randomBytes } from "node:crypto";
import { gzipSync } from "node:zlib";
import { mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

const DEFAULT_ITERATIONS = 210000;

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i += 1) {
    const key = argv[i];
    if (!key.startsWith("--")) continue;
    args[key.slice(2)] = argv[i + 1];
    i += 1;
  }
  return args;
}

async function readJsonIfExists(path, fallback = null) {
  try {
    return JSON.parse(await readFile(path, "utf8"));
  } catch (err) {
    if (err?.code === "ENOENT") return fallback;
    throw err;
  }
}

async function readJsonDir(dir) {
  const out = {};
  let entries = [];
  try {
    entries = await readdir(dir, { withFileTypes: true });
  } catch (err) {
    if (err?.code === "ENOENT") return out;
    throw err;
  }
  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.endsWith(".json") || entry.name === "index.json") continue;
    const key = entry.name.slice(0, -".json".length);
    out[key] = await readJsonIfExists(join(dir, entry.name), {});
  }
  return out;
}

function parsePositiveInt(value) {
  const parsed = Number.parseInt(String(value || ""), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
}

function selectRecentDates(index, days, maxDays) {
  const byIndex = (index?.days || []).map((day) => day?.date).filter(Boolean);
  const fallback = Object.keys(days || {}).sort().reverse();
  const seen = new Set();
  const ordered = [...byIndex, ...fallback].filter((date) => {
    if (seen.has(date)) return false;
    seen.add(date);
    return true;
  });
  return maxDays > 0 ? ordered.slice(0, maxDays) : ordered;
}

function filterObjectByDates(input, dates) {
  const allowed = new Set(dates);
  return Object.fromEntries(Object.entries(input || {}).filter(([date]) => allowed.has(date)));
}

function filterIndexDays(index, dates) {
  const allowed = new Set(dates);
  return { ...(index || {}), days: (index?.days || []).filter((day) => allowed.has(day?.date)) };
}

function filterDigestIndex(index, dates) {
  const allowed = new Set(dates);
  return { ...(index || {}), dates: (index?.dates || []).filter((date) => allowed.has(date)) };
}

async function readExternalDailyReports(input, dates) {
  const repoRoot = resolve(input, "..", "..");
  const allowed = new Set(dates);
  const notionDaily = await readJsonDir(join(repoRoot, "data", "notion-daily"));
  const out = {};
  for (const [date, payload] of Object.entries(notionDaily || {})) {
    if (!allowed.has(date) || !payload || typeof payload !== "object") continue;
    out[date] = { notion_daily_digest: payload };
  }
  return out;
}

function mergeExternalReportsIntoDigests(digests, externalReports) {
  const merged = { ...(digests || {}) };
  for (const [date, reports] of Object.entries(externalReports || {})) {
    merged[date] = { date, ...(merged[date] || {}), ...(reports || {}) };
  }
  return merged;
}

function encryptJson(payload, password) {
  const salt = randomBytes(16);
  const iv = randomBytes(12);
  const key = pbkdf2Sync(password, salt, DEFAULT_ITERATIONS, 32, "sha256");
  const cipher = createCipheriv("aes-256-gcm", key, iv);
  const plaintext = gzipSync(Buffer.from(JSON.stringify(payload), "utf8"), { level: 6 });
  const encrypted = Buffer.concat([cipher.update(plaintext), cipher.final()]);
  const tag = cipher.getAuthTag();
  return {
    version: 1,
    algorithm: "AES-GCM",
    compression: "gzip",
    kdf: {
      name: "PBKDF2",
      hash: "SHA-256",
      iterations: DEFAULT_ITERATIONS,
      salt: salt.toString("base64"),
    },
    iv: iv.toString("base64"),
    ciphertext: Buffer.concat([encrypted, tag]).toString("base64"),
  };
}

function buildPrivatePayload({ index, days, digestIndex, digests, selectedDates, externalReports, maxDays }) {
  const mergedDigests = mergeExternalReportsIntoDigests(digests, externalReports);
  return {
    generated_at: new Date().toISOString(),
    max_days: maxDays || null,
    index: filterIndexDays(index, selectedDates),
    days: filterObjectByDates(days, selectedDates),
    digest_index: filterDigestIndex(
      { ...digestIndex, dates: [...new Set([...(digestIndex?.dates || []), ...Object.keys(externalReports)])].sort().reverse() },
      selectedDates,
    ),
    digests: filterObjectByDates(mergedDigests, selectedDates),
  };
}

async function writeEncryptedJson(path, payload, password) {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, JSON.stringify(encryptJson(payload, password), null, 2) + "\n", "utf8");
}

async function writeSplitBundle(outputDir, payload, password) {
  await mkdir(outputDir, { recursive: true });
  await rm(join(outputDir, "day"), { recursive: true, force: true });
  await rm(join(outputDir, "digest"), { recursive: true, force: true });

  const dayFiles = {};
  const digestFiles = {};
  for (const [date, day] of Object.entries(payload.days || {})) {
    const file = `day/${date}.enc`;
    dayFiles[date] = file;
    await writeEncryptedJson(join(outputDir, file), day || { date, cards: [], items: [] }, password);
  }
  for (const [date, digest] of Object.entries(payload.digests || {})) {
    const file = `digest/${date}.enc`;
    digestFiles[date] = file;
    await writeEncryptedJson(join(outputDir, file), digest || { date }, password);
  }

  await writeEncryptedJson(join(outputDir, "manifest.enc"), {
    version: 1,
    format: "split-v1",
    generated_at: payload.generated_at,
    max_days: payload.max_days,
    index: payload.index,
    digest_index: payload.digest_index,
    days: dayFiles,
    digests: digestFiles,
  }, password);
}

async function main() {
  const args = parseArgs(process.argv);
  const input = args.input || "web/data";
  const output = args.output || "web/private/private.enc";
  const splitOutput = args["split-output"] || args.splitOutput;
  const password = process.env.PRIVATE_BUNDLE_PASSWORD || args.password;
  const maxDays = parsePositiveInt(args["max-days"] || args.maxDays);
  if (!password) {
    throw new Error("请设置 PRIVATE_BUNDLE_PASSWORD，或传入 --password。");
  }

  const index = await readJsonIfExists(join(input, "index.json"), { days: [] });
  const days = await readJsonDir(join(input, "day"));
  const digestIndex = await readJsonIfExists(join(input, "digest", "index.json"), { dates: [] });
  const digests = await readJsonDir(join(input, "digest"));
  const selectedDates = selectRecentDates(index, days, maxDays);
  const externalReports = await readExternalDailyReports(input, selectedDates);
  const payload = buildPrivatePayload({ index, days, digestIndex, digests, selectedDates, externalReports, maxDays });

  if (output) {
    await writeEncryptedJson(output, payload, password);
    console.log(`Wrote encrypted private bundle: ${output}`);
  }
  if (splitOutput) {
    await writeSplitBundle(splitOutput, payload, password);
    console.log(`Wrote split private bundle: ${splitOutput}`);
  }
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
