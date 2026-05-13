#!/usr/bin/env node
import { createCipheriv, pbkdf2Sync, randomBytes } from "node:crypto";
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

function encryptJson(payload, password) {
  const salt = randomBytes(16);
  const iv = randomBytes(12);
  const key = pbkdf2Sync(password, salt, DEFAULT_ITERATIONS, 32, "sha256");
  const cipher = createCipheriv("aes-256-gcm", key, iv);
  const plaintext = Buffer.from(JSON.stringify(payload), "utf8");
  const encrypted = Buffer.concat([cipher.update(plaintext), cipher.final()]);
  const tag = cipher.getAuthTag();
  return {
    version: 1,
    algorithm: "AES-GCM",
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
  if (!password) {
    throw new Error("请设置 PRIVATE_BUNDLE_PASSWORD，或传入 --password。");
  }

  const payload = {
    generated_at: new Date().toISOString(),
    index: await readJsonIfExists(join(input, "index.json"), { days: [] }),
    days: await readJsonDir(join(input, "day")),
    digest_index: await readJsonIfExists(join(input, "digest", "index.json"), { dates: [] }),
    digests: await readJsonDir(join(input, "digest")),
  };

  await mkdir(dirname(output), { recursive: true });
  await writeFile(output, JSON.stringify(encryptJson(payload, password), null, 2) + "\n", "utf8");
  console.log(`Wrote encrypted private bundle: ${output}`);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
