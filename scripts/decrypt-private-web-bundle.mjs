#!/usr/bin/env node
import { createDecipheriv, pbkdf2Sync } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { gunzipSync } from "node:zlib";

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i += 1) {
    const key = argv[i];
    if (!key.startsWith("--")) continue;
    const name = key.slice(2);
    if (name === "help") args.help = true;
    else {
      args[name] = argv[i + 1];
      i += 1;
    }
  }
  return args;
}

function usage() {
  return `Usage: PRIVATE_BUNDLE_PASSWORD='...' node scripts/decrypt-private-web-bundle.mjs [--input web/private/private.enc] [--output private.json]\n\nDecrypts private.enc locally for verification. The bundle uses AES-GCM with PBKDF2.\n`;
}

function b64(value) {
  return Buffer.from(String(value || ""), "base64");
}

function decryptEnvelope(envelope, password) {
  if (!envelope || envelope.version !== 1 || envelope.algorithm !== "AES-GCM") {
    throw new Error("私有数据包格式不支持");
  }
  const kdf = envelope.kdf || {};
  const combined = b64(envelope.ciphertext);
  if (combined.length < 17) throw new Error("私有数据包内容不完整");
  const encrypted = combined.subarray(0, -16);
  const tag = combined.subarray(-16);
  const key = pbkdf2Sync(password, b64(kdf.salt), Number(kdf.iterations || 210000), 32, "sha256");
  const decipher = createDecipheriv("aes-256-gcm", key, b64(envelope.iv));
  decipher.setAuthTag(tag);
  const plaintext = Buffer.concat([decipher.update(encrypted), decipher.final()]);
  const decoded = envelope.compression === "gzip" ? gunzipSync(plaintext) : plaintext;
  return JSON.parse(decoded.toString("utf8"));
}

async function main() {
  const args = parseArgs(process.argv);
  if (args.help) {
    process.stdout.write(usage());
    return;
  }
  const input = args.input || "web/private/private.enc";
  const output = args.output;
  const password = process.env.PRIVATE_BUNDLE_PASSWORD || args.password;
  if (!password) throw new Error("请设置 PRIVATE_BUNDLE_PASSWORD，或传入 --password。");
  const envelope = JSON.parse(await readFile(input, "utf8"));
  const payload = decryptEnvelope(envelope, password);
  const json = JSON.stringify(payload, null, 2) + "\n";
  if (output) await writeFile(output, json, "utf8");
  else process.stdout.write(json);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
