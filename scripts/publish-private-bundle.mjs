#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const DEFAULT_OUTPUT = "web/private/private.enc";

function parseArgs(argv) {
  const args = { commit: false, push: false };
  for (let i = 2; i < argv.length; i += 1) {
    const key = argv[i];
    if (!key.startsWith("--")) continue;
    const name = key.slice(2);
    if (["commit", "push", "help"].includes(name)) args[name] = true;
    else {
      args[name] = argv[i + 1];
      i += 1;
    }
  }
  return args;
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { stdio: "inherit", ...options });
  if (result.status !== 0) {
    const rendered = [command, ...args].join(" ");
    throw new Error(`${rendered} failed with exit code ${result.status}`);
  }
}

function runGit(args, options = {}) {
  return spawnSync("git", args, { stdio: "inherit", ...options });
}

function usage() {
  return `Usage:\n  PRIVATE_BUNDLE_PASSWORD='...' node scripts/publish-private-bundle.mjs [--input web/data] [--output web/private/private.enc] [--commit] [--push]\n\nThis builds the encrypted private bundle, optionally commits it with git, and optionally pushes it.\nOnly ${DEFAULT_OUTPUT} should be published; do not commit raw web/data or secrets.\n`;
}

async function main() {
  const args = parseArgs(process.argv);
  if (args.help) {
    process.stdout.write(usage());
    return;
  }
  if (!process.env.PRIVATE_BUNDLE_PASSWORD && !args.password) {
    throw new Error("请设置 PRIVATE_BUNDLE_PASSWORD，或传入 --password。");
  }
  const input = args.input || "web/data";
  const output = args.output || DEFAULT_OUTPUT;
  const buildArgs = [`${__dirname}/build-private-web-bundle.mjs`, "--input", input, "--output", output];
  if (args.password) buildArgs.push("--password", args.password);
  run("node", buildArgs);

  if (args.commit || args.push) {
    run("git", ["add", output]);
  }
  if (args.commit) {
    const message = args.message || "Update private encrypted bundle";
    const commit = runGit(["commit", "-m", message]);
    if (commit.status !== 0) {
      console.error("git commit 没有成功。通常是因为 private.enc 没有变化；如果是报错，请先处理 git 输出里的原因。");
      process.exit(commit.status || 1);
    }
  }
  if (args.push) {
    run("git", ["push"]);
  }
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
