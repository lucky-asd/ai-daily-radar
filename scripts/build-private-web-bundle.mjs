#!/usr/bin/env node
import { createCipheriv, createDecipheriv, pbkdf2Sync, randomBytes } from "node:crypto";
import { gunzipSync, gzipSync } from "node:zlib";
import { mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

const DEFAULT_ITERATIONS = 210000;

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i += 1) {
    const key = argv[i];
    if (!key.startsWith("--")) continue;
    const name = key.slice(2);
    if (["skip-monolith", "flat-parts"].includes(name)) {
      args[name] = true;
      continue;
    }
    args[name] = argv[i + 1];
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

function decryptJson(envelope, password) {
  const salt = Buffer.from(envelope.kdf.salt, "base64");
  const iv = Buffer.from(envelope.iv, "base64");
  const combined = Buffer.from(envelope.ciphertext, "base64");
  const encrypted = combined.subarray(0, combined.length - 16);
  const tag = combined.subarray(combined.length - 16);
  const key = pbkdf2Sync(password, salt, envelope.kdf.iterations, 32, "sha256");
  const decipher = createDecipheriv("aes-256-gcm", key, iv);
  decipher.setAuthTag(tag);
  const compressed = Buffer.concat([decipher.update(encrypted), decipher.final()]);
  return JSON.parse(gunzipSync(compressed).toString("utf8"));
}

function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
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

async function writeEncryptedJsonIfChanged(path, payload, password) {
  const current = await readJsonIfExists(path);
  if (current) {
    try {
      if (stableJson(decryptJson(current, password)) === stableJson(payload)) return false;
    } catch {
      // Password or envelope changed; replace this part with a fresh envelope.
    }
  }
  await writeEncryptedJson(path, payload, password);
  return true;
}

async function removeUnselectedParts(dir, selectedFiles, shouldManage = () => true) {
  const allowed = new Set(selectedFiles);
  const removed = [];
  let entries = [];
  try {
    entries = await readdir(dir, { withFileTypes: true });
  } catch (err) {
    if (err?.code === "ENOENT") return removed;
    throw err;
  }
  for (const entry of entries) {
    if (entry.isFile() && entry.name.endsWith(".enc") && shouldManage(entry.name) && !allowed.has(entry.name)) {
      await rm(join(dir, entry.name), { force: true });
      removed.push(entry.name);
    }
  }
  return removed;
}

async function writeSplitBundle(outputDir, payload, password, flatParts = false) {
  await mkdir(outputDir, { recursive: true });

  const dayFiles = {};
  const digestFiles = {};
  let changedDays = 0;
  let changedDigests = 0;
  const changedFiles = [];
  for (const [date, day] of Object.entries(payload.days || {})) {
    const file = flatParts ? `day-${date}.enc` : `day/${date}.enc`;
    dayFiles[date] = file;
    if (await writeEncryptedJsonIfChanged(
      join(outputDir, file),
      day || { date, cards: [], items: [] },
      password,
    )) {
      changedDays += 1;
      changedFiles.push(file);
    }
  }
  for (const [date, digest] of Object.entries(payload.digests || {})) {
    const file = flatParts ? `digest-${date}.enc` : `digest/${date}.enc`;
    digestFiles[date] = file;
    if (await writeEncryptedJsonIfChanged(join(outputDir, file), digest || { date }, password)) {
      changedDigests += 1;
      changedFiles.push(file);
    }
  }

  let removedFiles = [];
  if (flatParts) {
    removedFiles = await removeUnselectedParts(
      outputDir,
      [...Object.values(dayFiles), ...Object.values(digestFiles)],
      (name) => name.startsWith("day-") || name.startsWith("digest-"),
    );
  } else {
    const removedDays = await removeUnselectedParts(
      join(outputDir, "day"),
      Object.values(dayFiles).map((file) => file.split("/").pop()),
    );
    const removedDigests = await removeUnselectedParts(
      join(outputDir, "digest"),
      Object.values(digestFiles).map((file) => file.split("/").pop()),
    );
    removedFiles = [
      ...removedDays.map((file) => `day/${file}`),
      ...removedDigests.map((file) => `digest/${file}`),
    ];
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
  changedFiles.push("manifest.enc");
  return { changedDays, changedDigests, changedFiles, removedFiles };
}

async function main() {
  const args = parseArgs(process.argv);
  const input = args.input || "web/data";
  const output = args["skip-monolith"] ? null : (args.output || "web/private/private.enc");
  const splitOutput = args["split-output"] || args.splitOutput;
  const password = process.env.PRIVATE_BUNDLE_PASSWORD || args.password;
  const maxDays = parsePositiveInt(args["max-days"] || args.maxDays);
  const flatParts = args["flat-parts"] === true;
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
    const changes = await writeSplitBundle(splitOutput, payload, password, flatParts);
    console.log(
      `Wrote split private bundle: ${splitOutput} `
      + `(changed days: ${changes.changedDays}, digests: ${changes.changedDigests})`,
    );
    if (args["changes-output"]) {
      await mkdir(dirname(args["changes-output"]), { recursive: true });
      await writeFile(args["changes-output"], JSON.stringify(changes, null, 2) + "\n", "utf8");
    }
  }
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
