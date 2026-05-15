#!/usr/bin/env node
import { createCipheriv, pbkdf2Sync, randomBytes } from "node:crypto";
import { gzipSync } from "node:zlib";
import { mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";

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

async function main() {
  const args = parseArgs(process.argv);
  const input = args.input || "web/data";
  const output = args.output || "web/private/private.enc";
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
  const payload = {
    generated_at: new Date().toISOString(),
    max_days: maxDays || null,
    index: filterIndexDays(index, selectedDates),
    days: filterObjectByDates(days, selectedDates),
    digest_index: filterDigestIndex(digestIndex, selectedDates),
    digests: filterObjectByDates(digests, selectedDates),
  };

  await mkdir(dirname(output), { recursive: true });
  await writeFile(output, JSON.stringify(encryptJson(payload, password), null, 2) + "\n", "utf8");
  console.log(`Wrote encrypted private bundle: ${output}`);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
