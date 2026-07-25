#!/usr/bin/env node

import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { spawnSync } from "node:child_process";

function reply(message) {
  process.stdout.write(
    `${JSON.stringify({
      continue: false,
      stopReason: "Nagare swarm guard failed",
      systemMessage: message,
    })}\n`,
  );
}

if (!process.env.SWARM_TASK_ID) {
  process.exit(0);
}

let input = {};
try {
  input = JSON.parse(readFileSync(0, "utf8") || "{}");
} catch {
  reply("The swarm guard could not parse the Codex hook payload.");
  process.exit(0);
}

const cwd = input.cwd || process.cwd();
const owned = JSON.parse(process.env.SWARM_OWNED_PATHS || "[]");
const frozen = JSON.parse(process.env.SWARM_FROZEN_TESTS || "{}");
const protectedFiles = new Set([
  "go.mod",
  "go.sum",
  "web/package.json",
  "web/package-lock.json",
  "package.json",
  "package-lock.json",
]);

const status = spawnSync(
  "git",
  ["status", "--porcelain=v1", "--untracked-files=all"],
  { cwd, encoding: "utf8" },
);
if (status.status !== 0) {
  reply(`The swarm guard could not inspect git status: ${status.stderr.trim()}`);
  process.exit(0);
}

const changed = status.stdout
  .split("\n")
  .filter(Boolean)
  .map((line) => line.slice(3))
  .map((path) => (path.includes(" -> ") ? path.split(" -> ").at(-1) : path));

const allowed = (path) =>
  owned.some((entry) =>
    entry.endsWith("/") ? path.startsWith(entry) : path === entry,
  );

const outside = changed.filter((path) => !allowed(path));
if (outside.length > 0) {
  reply(`Files outside the owned paths changed: ${outside.join(", ")}`);
  process.exit(0);
}

const dependencyChanges = changed.filter((path) => protectedFiles.has(path));
if (
  dependencyChanges.length > 0 &&
  process.env.SWARM_ALLOW_DEPENDENCIES !== "1"
) {
  reply(
    `Dependency manifests are not approved for this task: ${dependencyChanges.join(", ")}`,
  );
  process.exit(0);
}

for (const [path, expected] of Object.entries(frozen)) {
  let contents;
  try {
    contents = readFileSync(path);
  } catch {
    reply(`Frozen evaluator test was deleted or moved: ${path}`);
    process.exit(0);
  }
  const actual = createHash("sha256").update(contents).digest("hex");
  if (actual !== expected) {
    reply(`Frozen evaluator test was modified: ${path}`);
    process.exit(0);
  }
}

process.exit(0);
