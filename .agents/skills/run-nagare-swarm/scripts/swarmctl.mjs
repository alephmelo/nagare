#!/usr/bin/env node

import { createHash, randomUUID } from "node:crypto";
import {
  existsSync,
  lstatSync,
  mkdtempSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  readlinkSync,
  renameSync,
  rmSync,
  statSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn, spawnSync } from "node:child_process";

const releaseDir = resolve(
  process.env.SWARM_RELEASE ||
    join(dirname(fileURLToPath(import.meta.url)), ".."),
);
const swarmHome = resolve(
  process.env.SWARM_HOME ||
    join(process.env.HOME || process.cwd(), ".local/share/nagare-swarm"),
);
const repo = resolve(process.env.SWARM_REPO || process.cwd());
const backlogPath = existsSync(join(releaseDir, "backlog.json"))
  ? join(releaseDir, "backlog.json")
  : join(repo, "swarm", "backlog.json");
const schemaDir = existsSync(join(releaseDir, "references"))
  ? join(releaseDir, "references")
  : join(
      repo,
      ".agents",
      "skills",
      "run-nagare-swarm",
      "references",
    );
const runsDir = join(swarmHome, "runs");
const worktreesDir = join(swarmHome, "worktrees");
const activeRunPath = join(swarmHome, "active-run");
const lockPath = join(swarmHome, "state.lock");
const codexBin = process.env.CODEX_BIN || "codex";
const safeCommandPath = [
  "/Applications/Xcode.app/Contents/Developer/usr/bin",
  "/opt/homebrew/bin",
  "/usr/local/bin",
  "/usr/bin",
  "/bin",
  "/usr/sbin",
  "/sbin",
  "/Library/Apple/usr/bin",
]
  .filter((path) => existsSync(path))
  .join(":");
const dependencyFiles = new Set([
  "go.mod",
  "go.sum",
  "web/package.json",
  "web/package-lock.json",
  "package.json",
  "package-lock.json",
]);
const runtimeBundleFiles = [
  "nagare",
  "nagare.yaml",
  "dags/architecture-swarm.yaml",
  "backlog.json",
  "scripts/swarmctl.mjs",
  "references/planner.schema.json",
  "references/agent.schema.json",
  "references/reviewer.schema.json",
];
const controlFileMappings = {
  "nagare.yaml": "swarm/nagare-swarm.yaml",
  "dags/architecture-swarm.yaml": "swarm/dags/architecture-swarm.yaml",
  "backlog.json": "swarm/backlog.json",
  "scripts/swarmctl.mjs":
    ".agents/skills/run-nagare-swarm/scripts/swarmctl.mjs",
  "references/planner.schema.json":
    ".agents/skills/run-nagare-swarm/references/planner.schema.json",
  "references/agent.schema.json":
    ".agents/skills/run-nagare-swarm/references/agent.schema.json",
  "references/reviewer.schema.json":
    ".agents/skills/run-nagare-swarm/references/reviewer.schema.json",
};
let appleToolchainEnvironment;

function log(message) {
  process.stdout.write(`[swarm] ${message}\n`);
}

function fail(message) {
  throw new Error(message);
}

function sleep(milliseconds) {
  Atomics.wait(
    new Int32Array(new SharedArrayBuffer(4)),
    0,
    0,
    milliseconds,
  );
}

async function terminateProcessGroup(pid) {
  if (!Number.isInteger(pid) || pid < 1 || process.platform === "win32") {
    return;
  }
  try {
    process.kill(-pid, "SIGTERM");
  } catch (error) {
    if (error.code === "ESRCH") return;
    throw error;
  }
  await new Promise((resolvePromise) => setTimeout(resolvePromise, 250));
  try {
    process.kill(-pid, "SIGKILL");
  } catch (error) {
    if (error.code !== "ESRCH") throw error;
  }
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd || repo,
    encoding: "utf8",
    env: options.env || process.env,
    stdio: options.inherit ? "inherit" : "pipe",
  });
  if (result.status !== 0 && !options.allowFailure) {
    const detail = [result.stdout, result.stderr]
      .filter(Boolean)
      .join("\n")
      .trim();
    fail(
      `${command} ${args.join(" ")} failed with exit ${result.status}${detail ? `\n${detail}` : ""}`,
    );
  }
  return result;
}

function git(args, cwd = repo, options = {}) {
  return run("git", args, { cwd, ...options });
}

function gitText(args, cwd = repo) {
  return git(args, cwd).stdout.trim();
}

function readJSON(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function writeJSONAtomic(path, value) {
  mkdirSync(dirname(path), { recursive: true });
  const temp = `${path}.${process.pid}.tmp`;
  writeFileSync(temp, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
  renameSync(temp, path);
}

function acquireLock() {
  mkdirSync(swarmHome, { recursive: true });
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    try {
      mkdirSync(lockPath);
      writeFileSync(
        join(lockPath, "owner.json"),
        `${JSON.stringify({ pid: process.pid, at: new Date().toISOString() })}\n`,
      );
      return;
    } catch (error) {
      if (error.code !== "EEXIST") throw error;
      try {
        if (Date.now() - statSync(lockPath).mtimeMs > 10 * 60_000) {
          rmSync(lockPath, { recursive: true, force: true });
          continue;
        }
      } catch {
        // Another process may be replacing the lock; retry.
      }
      sleep(100);
    }
  }
  fail(`Timed out waiting for swarm state lock ${lockPath}`);
}

function withLock(callback) {
  acquireLock();
  try {
    return callback();
  } finally {
    rmSync(lockPath, { recursive: true, force: true });
  }
}

function activeRunID() {
  if (!existsSync(activeRunPath)) fail("No active swarm run. Trigger the DAG.");
  return readFileSync(activeRunPath, "utf8").trim();
}

function statePath(runID = activeRunID()) {
  return join(runsDir, runID, "state.json");
}

function readState(runID = activeRunID()) {
  return readJSON(statePath(runID));
}

function updateState(mutator) {
  return withLock(() => {
    const runID = activeRunID();
    const state = readState(runID);
    const result = mutator(state);
    state.updatedAt = new Date().toISOString();
    writeJSONAtomic(statePath(runID), state);
    return result === undefined ? state : result;
  });
}

function processIsAlive(pid) {
  if (!Number.isInteger(pid) || pid < 1) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function leaseIsActive(lease) {
  return (
    lease &&
    typeof lease.owner === "string" &&
    new Date(lease.expiresAt).getTime() > Date.now() &&
    processIsAlive(lease.pid)
  );
}

function claimTask(taskID) {
  return withLock(() => {
    const runID = activeRunID();
    const state = readState(runID);
    const task = state.taskSpecs.find((candidate) => candidate.id === taskID);
    if (!task) fail(`Unknown task ${taskID}`);
    const taskState = state.tasks[taskID];
    if (["completed", "integrated"].includes(taskState.status)) {
      return { skip: true, status: taskState.status };
    }
    if (taskState.status === "running" && leaseIsActive(taskState.lease)) {
      fail(
        `${taskID}: already owned by live runner ${taskState.lease.owner}`,
      );
    }
    for (const dependency of task.dependsOn) {
      if (state.tasks[dependency].status !== "integrated") {
        fail(`${taskID}: dependency ${dependency} is not integrated`);
      }
    }

    const lease = {
      owner: randomUUID(),
      pid: process.pid,
      claimedAt: new Date().toISOString(),
      expiresAt: new Date(Date.now() + 7 * 60 * 60_000).toISOString(),
    };
    const attempt = taskState.attempts + 1;
    taskState.status = "running";
    taskState.attempts = attempt;
    taskState.baseCommit = state.integrationHead;
    taskState.error = null;
    taskState.lease = lease;
    state.updatedAt = new Date().toISOString();
    writeJSONAtomic(statePath(runID), state);
    return {
      skip: false,
      initial: structuredClone(state),
      task,
      attempt,
      baseCommit: state.integrationHead,
      lease,
    };
  });
}

function updateClaimedTask(taskID, owner, mutator) {
  return updateState((state) => {
    if (state.tasks[taskID].lease?.owner !== owner) {
      fail(`${taskID}: task lease ownership was lost`);
    }
    mutator(state.tasks[taskID], state);
  });
}

function claimIntegration(wave) {
  return withLock(() => {
    const runID = activeRunID();
    const state = readState(runID);
    if (state.integratedWaves.includes(wave)) {
      return { skip: true };
    }
    state.integrationLeases ||= {};
    if (leaseIsActive(state.integrationLeases[wave])) {
      fail(
        `wave ${wave}: already owned by live runner ${state.integrationLeases[wave].owner}`,
      );
    }
    const tasks = state.taskSpecs.filter((task) => task.wave === wave);
    if (tasks.length === 0) fail(`No tasks in wave ${wave}`);
    for (const task of tasks) {
      if (state.tasks[task.id].status !== "completed") {
        fail(`wave ${wave}: task ${task.id} is not completed`);
      }
    }
    const lease = {
      owner: randomUUID(),
      pid: process.pid,
      claimedAt: new Date().toISOString(),
      expiresAt: new Date(Date.now() + 3 * 60 * 60_000).toISOString(),
    };
    state.integrationLeases[wave] = lease;
    state.updatedAt = new Date().toISOString();
    writeJSONAtomic(statePath(runID), state);
    return {
      skip: false,
      initial: structuredClone(state),
      tasks,
      lease,
    };
  });
}

function updateClaimedIntegration(wave, owner, mutator) {
  return updateState((state) => {
    if (state.integrationLeases?.[wave]?.owner !== owner) {
      fail(`wave ${wave}: integration lease ownership was lost`);
    }
    mutator(state);
  });
}

function claimFinalization() {
  return withLock(() => {
    const runID = activeRunID();
    const state = readState(runID);
    if (state.status === "completed") {
      return { skip: true, branch: state.branch };
    }
    if (leaseIsActive(state.finalizeLease)) {
      fail(
        `finalize: already owned by live runner ${state.finalizeLease.owner}`,
      );
    }
    const incomplete = Object.entries(state.tasks)
      .filter(([, task]) => task.status !== "integrated")
      .map(([id]) => id);
    if (incomplete.length > 0) {
      fail(`Cannot finalize; tasks are not integrated: ${incomplete.join(", ")}`);
    }
    const lease = {
      owner: randomUUID(),
      pid: process.pid,
      claimedAt: new Date().toISOString(),
      expiresAt: new Date(Date.now() + 30 * 60_000).toISOString(),
    };
    state.finalizeLease = lease;
    state.updatedAt = new Date().toISOString();
    writeJSONAtomic(statePath(runID), state);
    return { skip: false, initial: structuredClone(state), lease };
  });
}

function loadBacklog() {
  if (!existsSync(backlogPath)) fail(`Missing runtime backlog: ${backlogPath}`);
  return readJSON(backlogPath);
}

function isSafeRelativePath(path) {
  return (
    typeof path === "string" &&
    path.length > 0 &&
    !path.startsWith("/") &&
    !path.split("/").includes("..") &&
    path !== ".git" &&
    !path.startsWith(".git/")
  );
}

function pathIsOwned(path, ownedPaths) {
  return ownedPaths.some((entry) =>
    entry.endsWith("/") ? path.startsWith(entry) : path === entry,
  );
}

function isTestFile(path) {
  return (
    /_test\.go$/.test(path) ||
    /\.(test|spec)\.(js|jsx|ts|tsx)$/.test(path)
  );
}

function validateBacklog(backlog) {
  if (backlog.version !== 1) fail("backlog.version must be 1");
  if (!Array.isArray(backlog.tasks) || backlog.tasks.length === 0) {
    fail("backlog.tasks must be a non-empty array");
  }
  if (
    !Number.isInteger(backlog.maxParallelTasks) ||
    backlog.maxParallelTasks < 1 ||
    backlog.maxParallelTasks > 4
  ) {
    fail("backlog.maxParallelTasks must be between 1 and 4");
  }
  if (
    !Number.isInteger(backlog.maxRepairAttempts) ||
    backlog.maxRepairAttempts < 0 ||
    backlog.maxRepairAttempts > 3
  ) {
    fail("backlog.maxRepairAttempts must be between 0 and 3");
  }
  if (
    !Array.isArray(backlog.globalChecks) ||
    backlog.globalChecks.length === 0 ||
    backlog.globalChecks.some((check) => typeof check !== "string" || !check)
  ) {
    fail("backlog.globalChecks must contain shell commands");
  }

  const ids = new Set();
  for (const task of backlog.tasks) {
    if (!/^[a-z0-9][a-z0-9-]*$/.test(task.id || "")) {
      fail(`Invalid task id: ${task.id}`);
    }
    if (ids.has(task.id)) fail(`Duplicate task id: ${task.id}`);
    ids.add(task.id);
    if (!Number.isInteger(task.wave) || task.wave < 1) {
      fail(`Task ${task.id} has an invalid wave`);
    }
    for (const field of ["objective", "architecture"]) {
      if (typeof task[field] !== "string" || !task[field]) {
        fail(`Task ${task.id} is missing ${field}`);
      }
    }
    for (const field of [
      "dependsOn",
      "acceptance",
      "nonGoals",
      "ownedPaths",
      "checks",
    ]) {
      if (!Array.isArray(task[field])) {
        fail(`Task ${task.id}.${field} must be an array`);
      }
    }
    if (task.ownedPaths.length === 0 || task.checks.length === 0) {
      fail(`Task ${task.id} needs owned paths and targeted checks`);
    }
    for (const path of task.ownedPaths) {
      if (!isSafeRelativePath(path)) {
        fail(`Task ${task.id} has unsafe owned path: ${path}`);
      }
    }
  }

  for (const task of backlog.tasks) {
    for (const dependency of task.dependsOn) {
      if (!ids.has(dependency)) {
        fail(`Task ${task.id} depends on unknown task ${dependency}`);
      }
      const predecessor = backlog.tasks.find(
        (candidate) => candidate.id === dependency,
      );
      if (predecessor.wave >= task.wave) {
        fail(
          `Task ${task.id} must depend only on tasks from an earlier wave`,
        );
      }
    }
  }
}

function createManifest(sourceCommit, uncommittedEntryCount) {
  if (!/^[0-9a-f]{40}$/.test(sourceCommit || "")) {
    fail("create-manifest requires a full source commit SHA");
  }
  const files = {};
  for (const path of runtimeBundleFiles) {
    const absolute = join(releaseDir, path);
    if (!existsSync(absolute)) fail(`Missing release file ${path}`);
    files[path] = sha256File(absolute);
  }
  writeJSONAtomic(join(releaseDir, "manifest.json"), {
    version: 1,
    sourceCommit,
    uncommittedEntryCountAtInstall: Number.parseInt(
      uncommittedEntryCount || "0",
      10,
    ),
    installedAt: new Date().toISOString(),
    files,
  });
  log(`Wrote release manifest for ${sourceCommit}`);
}

function validateManifest() {
  const manifestPath = join(releaseDir, "manifest.json");
  if (!existsSync(manifestPath)) {
    if (process.env.SWARM_RELEASE) {
      fail(`Pinned release is missing manifest: ${manifestPath}`);
    }
    return null;
  }
  const manifest = readJSON(manifestPath);
  if (
    manifest.version !== 1 ||
    !/^[0-9a-f]{40}$/.test(manifest.sourceCommit || "") ||
    typeof manifest.files !== "object"
  ) {
    fail("Pinned release manifest is malformed");
  }
  for (const path of runtimeBundleFiles) {
    const expected = manifest.files[path];
    const absolute = join(releaseDir, path);
    if (!expected || !existsSync(absolute)) {
      fail(`Pinned release manifest is missing ${path}`);
    }
    if (sha256File(absolute) !== expected) {
      fail(`Pinned release integrity check failed for ${path}`);
    }
  }
  return manifest;
}

function validateRuntime() {
  const backlog = loadBacklog();
  validateBacklog(backlog);
  for (const path of [
    join(schemaDir, "planner.schema.json"),
    join(schemaDir, "agent.schema.json"),
    join(schemaDir, "reviewer.schema.json"),
  ]) {
    readJSON(path);
  }
  const runtimeDAG = join(releaseDir, "dags", "architecture-swarm.yaml");
  const dag = existsSync(runtimeDAG)
    ? runtimeDAG
    : join(repo, "swarm", "dags", "architecture-swarm.yaml");
  if (!existsSync(dag)) fail(`Missing runtime DAG: ${dag}`);
  for (const task of backlog.tasks) {
    const dagText = readFileSync(dag, "utf8");
    if (!dagText.includes(`execute-task ${task.id}`)) {
      fail(`DAG does not execute backlog task ${task.id}`);
    }
  }
  validateManifest();
  return backlog;
}

function preflightReady() {
  const inside = gitText(["rev-parse", "--is-inside-work-tree"]);
  if (inside !== "true") fail(`${repo} is not a git worktree`);

  const dirtyTracked = gitText([
    "status",
    "--porcelain=v1",
    "--untracked-files=no",
  ]);
  if (dirtyTracked) {
    fail(
      "The primary checkout has uncommitted tracked changes. Commit or stash them before triggering the swarm.",
    );
  }

  const requiredTracked = [
    "AGENTS.md",
    ".codex/config.toml",
    ".codex/hooks.json",
    ".codex/hooks/swarm-stop-check.mjs",
    ".codex/agents/swarm-foreman.toml",
    ".codex/agents/swarm-planner.toml",
    ".codex/agents/swarm-eval-author.toml",
    ".codex/agents/swarm-implementer.toml",
    ".codex/agents/swarm-reviewer.toml",
    ".codex/agents/swarm-integrator.toml",
    "scripts/nagare-swarm",
    "swarm/backlog.json",
    "swarm/nagare-swarm.yaml",
    "swarm/dags/architecture-swarm.yaml",
    ".agents/skills/run-nagare-swarm/SKILL.md",
    ".agents/skills/run-nagare-swarm/agents/openai.yaml",
    ".agents/skills/run-nagare-swarm/references/operations.md",
    ".agents/skills/run-nagare-swarm/references/planner.schema.json",
    ".agents/skills/run-nagare-swarm/references/agent.schema.json",
    ".agents/skills/run-nagare-swarm/references/reviewer.schema.json",
    ".agents/skills/run-nagare-swarm/scripts/swarmctl.mjs",
  ];
  for (const path of requiredTracked) {
    git(["ls-files", "--error-unmatch", path]);
  }

  const manifest = validateManifest();
  if (!manifest) fail("Ready checks require an installed pinned release");
  const ancestor = git(
    ["merge-base", "--is-ancestor", manifest.sourceCommit, "HEAD"],
    repo,
    { allowFailure: true },
  );
  if (ancestor.status !== 0) {
    fail(
      `Pinned binary source ${manifest.sourceCommit} is not an ancestor of the current HEAD`,
    );
  }
  for (const [runtimePath, sourcePath] of Object.entries(
    controlFileMappings,
  )) {
    if (
      sha256File(join(releaseDir, runtimePath)) !==
      sha256File(join(repo, sourcePath))
    ) {
      fail(
        `Pinned control file ${runtimePath} differs from committed ${sourcePath}; install a new release`,
      );
    }
  }

  run(codexBin, ["--version"]);
  if (process.platform === "darwin" && !existsSync("/usr/bin/sandbox-exec")) {
    fail("Unattended checks require /usr/bin/sandbox-exec on macOS");
  }
  git(["diff", "--check"]);
}

function timestampID() {
  return new Date()
    .toISOString()
    .replace(/\.\d{3}Z$/, "Z")
    .replaceAll(":", "")
    .replaceAll("-", "");
}

function prepareTrigger() {
  validateRuntime();
  preflightReady();
  mkdirSync(runsDir, { recursive: true });

  return withLock(() => {
    if (!existsSync(activeRunPath)) {
      log("No runner state needs recovery before launch");
      return;
    }

    const currentID = readFileSync(activeRunPath, "utf8").trim();
    const currentPath = statePath(currentID);
    if (!existsSync(currentPath)) {
      rmSync(activeRunPath, { force: true });
      log(`Cleared stale active-run pointer ${currentID}`);
      return;
    }

    const current = readJSON(currentPath);
    const head = gitText(["rev-parse", "HEAD"]);
    if (current.status === "running" && current.baseCommit === head) {
      log(`Runner state ${currentID} is resumable at ${head}`);
      return;
    }
    if (current.status === "running") {
      current.status = "abandoned";
      current.abandonedAt = new Date().toISOString();
      current.abandonedReason =
        `A new committed control release superseded ${current.baseCommit}`;
      writeJSONAtomic(currentPath, current);
      log(`Archived interrupted runner state ${currentID}`);
    } else {
      log(`Archived ${current.status} runner state ${currentID}`);
    }
    rmSync(activeRunPath, { force: true });
  });
}

function initializeRun() {
  const backlog = validateRuntime();
  preflightReady();
  mkdirSync(runsDir, { recursive: true });
  mkdirSync(worktreesDir, { recursive: true });

  return withLock(() => {
    if (existsSync(activeRunPath)) {
      const currentID = readFileSync(activeRunPath, "utf8").trim();
      const currentPath = statePath(currentID);
      if (existsSync(currentPath)) {
        const current = readJSON(currentPath);
        if (current.status === "running") {
          const head = gitText(["rev-parse", "HEAD"]);
          if (current.baseCommit !== head) {
            fail(
              `Active run ${currentID} is based on ${current.baseCommit}; the checkout is now ${head}`,
            );
          }
          log(`Reusing active run ${currentID}`);
          return current;
        }
      }
    }

    const baseCommit = gitText(["rev-parse", "HEAD"]);
    const runID = `${timestampID()}-${baseCommit.slice(0, 10)}`;
    const taskState = Object.fromEntries(
      backlog.tasks.map((task) => [
        task.id,
        {
          status: "pending",
          attempts: 0,
          wave: task.wave,
          evalCommit: null,
          implementationCommit: null,
          worktree: null,
          error: null,
          lease: null,
        },
      ]),
    );
    const state = {
      version: 1,
      runID,
      name: backlog.name,
      status: "running",
      repo,
      releaseDir,
      baseCommit,
      integrationHead: baseCommit,
      integratedWaves: [],
      integrationLeases: {},
      branch: null,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      maxRepairAttempts: backlog.maxRepairAttempts,
      globalChecks: backlog.globalChecks,
      taskSpecs: backlog.tasks,
      tasks: taskState,
    };
    mkdirSync(join(runsDir, runID, "artifacts"), { recursive: true });
    writeJSONAtomic(statePath(runID), state);
    writeFileSync(activeRunPath, `${runID}\n`, { mode: 0o600 });
    log(`Initialized run ${runID} at ${baseCommit}`);
    return state;
  });
}

function changedPaths(cwd) {
  const output = git(
    ["status", "--porcelain=v1", "-z", "--untracked-files=all"],
    cwd,
  ).stdout;
  if (!output) return [];
  const records = output.split("\0");
  const paths = [];
  for (let index = 0; index < records.length; index += 1) {
    const record = records[index];
    if (!record) continue;
    if (record.length < 4 || record[2] !== " ") {
      fail(`Could not parse git porcelain record ${JSON.stringify(record)}`);
    }
    const status = record.slice(0, 2);
    paths.push(record.slice(3));
    if (status.includes("R") || status.includes("C")) index += 1;
  }
  return paths;
}

function trackedPaths(cwd) {
  const output = gitText(["ls-files"], cwd);
  return output ? output.split("\n") : [];
}

function sha256File(path) {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

function freezeTests(cwd, paths) {
  return Object.fromEntries(
    paths.map((path) => [path, sha256File(join(cwd, path))]),
  );
}

function validateScope(cwd, task, frozenTests = {}) {
  const changed = changedPaths(cwd);
  const outside = changed.filter(
    (path) => !pathIsOwned(path, task.ownedPaths),
  );
  if (outside.length > 0) {
    fail(
      `Task ${task.id} changed files outside its owned paths: ${outside.join(", ")}`,
    );
  }
  const dependencyChanges = changed.filter((path) =>
    dependencyFiles.has(path),
  );
  if (dependencyChanges.length > 0 && !task.allowDependencies) {
    fail(
      `Task ${task.id} changed unapproved dependency files: ${dependencyChanges.join(", ")}`,
    );
  }
  for (const [path, expected] of Object.entries(frozenTests)) {
    const absolute = join(cwd, path);
    if (!existsSync(absolute)) fail(`Frozen test was deleted: ${path}`);
    if (sha256File(absolute) !== expected) {
      fail(`Frozen test was modified: ${path}`);
    }
  }
  return changed;
}

function makeWorktree(runID, name, baseCommit, attempt) {
  const root = join(worktreesDir, runID);
  mkdirSync(root, { recursive: true });
  const path = join(root, `${name}-attempt-${attempt}`);
  if (existsSync(path)) {
    fail(`Refusing to overwrite existing worktree ${path}`);
  }
  git(["worktree", "add", "--detach", path, baseCommit]);

  const sharedModules = join(repo, "web", "node_modules");
  const worktreeModules = join(path, "web", "node_modules");
  if (existsSync(sharedModules) && !existsSync(worktreeModules)) {
    symlinkSync(sharedModules, worktreeModules, "dir");
  }
  return path;
}

function cleanAgentEnvironment(task, frozenTests) {
  const env = {};
  for (const key of [
    "HOME",
    "USER",
    "LOGNAME",
    "SHELL",
    "TMPDIR",
    "LANG",
    "LC_ALL",
    "TERM",
    "CODEX_HOME",
    "CODEX_CA_CERTIFICATE",
    "SSL_CERT_FILE",
  ]) {
    if (process.env[key]) env[key] = process.env[key];
  }
  env.PATH = safeCommandPath;
  env.SWARM_TASK_ID = task.id;
  env.SWARM_OWNED_PATHS = JSON.stringify(task.ownedPaths);
  env.SWARM_FROZEN_TESTS = JSON.stringify(frozenTests);
  env.SWARM_ALLOW_DEPENDENCIES = task.allowDependencies ? "1" : "0";
  return env;
}

function tomlInlineTable(values) {
  return `{${Object.entries(values)
    .map(([key, value]) => `${key}=${JSON.stringify(value)}`)
    .join(",")}}`;
}

async function runCodex({
  role,
  cwd,
  prompt,
  schema,
  artifact,
  sandbox,
  task,
  frozenTests = {},
}) {
  const artifactDir = dirname(artifact);
  mkdirSync(artifactDir, { recursive: true });
  const shellRoot = mkdtempSync("/private/tmp/nagare-swarm-agent-");
  const shellHome = join(shellRoot, "home");
  const shellTemp = join(shellRoot, "tmp");
  const shellGoCache = join(shellRoot, "go-cache");
  const shellGoPath = join(shellRoot, "go-path");
  const shellNpmCache = join(shellRoot, "npm-cache");
  for (const path of [
    shellHome,
    shellTemp,
    shellGoCache,
    shellGoPath,
    shellNpmCache,
  ]) {
    mkdirSync(path, { recursive: true });
  }
  const { goModCache, goRoot } = toolchainReadablePaths(cwd);
  const shellEnvironment = tomlInlineTable({
    PATH: safeCommandPath,
    HOME: shellHome,
    TMPDIR: shellTemp,
    GOCACHE: shellGoCache,
    GOMODCACHE: goModCache,
    GOPATH: shellGoPath,
    GOROOT: goRoot,
    npm_config_cache: shellNpmCache,
    USER: "nagare-swarm",
    LOGNAME: "nagare-swarm",
    SHELL: "/bin/sh",
    LANG: process.env.LANG || "C.UTF-8",
    TERM: "dumb",
  });
  const args = [
    "--ask-for-approval",
    "never",
    "exec",
    "--ignore-user-config",
    "--disable",
    "apps",
    "--disable",
    "plugins",
    "--disable",
    "remote_plugin",
    "--disable",
    "in_app_browser",
    "--config",
    'shell_environment_policy.inherit="none"',
    "--config",
    `shell_environment_policy.set=${shellEnvironment}`,
    "--ephemeral",
    "--json",
    "--color",
    "never",
    "--sandbox",
    sandbox,
    "--output-schema",
    schema,
    "--output-last-message",
    artifact,
    "--cd",
    cwd,
    "-",
  ];
  log(`${task.id}: starting ${role} Codex session`);
  try {
    const exitCode = await new Promise((resolvePromise, rejectPromise) => {
      const child = spawn(codexBin, args, {
        cwd,
        env: cleanAgentEnvironment(task, frozenTests),
        stdio: ["pipe", "pipe", "pipe"],
        detached: process.platform !== "win32",
      });
      child.stdout.on("data", (chunk) => process.stdout.write(chunk));
      child.stderr.on("data", (chunk) => process.stderr.write(chunk));
      let settled = false;
      child.on("error", async (error) => {
        if (settled) return;
        settled = true;
        try {
          await terminateProcessGroup(child.pid);
        } catch (cleanupError) {
          error.cause = cleanupError;
        }
        rejectPromise(error);
      });
      child.on("exit", async (code) => {
        if (settled) return;
        settled = true;
        try {
          await terminateProcessGroup(child.pid);
          resolvePromise(code);
        } catch (error) {
          rejectPromise(error);
        }
      });
      child.stdin.end(prompt);
    });
    if (exitCode !== 0) {
      fail(`${task.id}: ${role} Codex session exited ${exitCode}`);
    }
    if (!existsSync(artifact)) {
      fail(`${task.id}: ${role} did not produce ${artifact}`);
    }
    const result = readJSON(artifact);
    log(`${task.id}: ${role} session completed`);
    return result;
  } finally {
    rmSync(shellRoot, { recursive: true, force: true });
  }
}

function seatbeltString(value) {
  return `"${value.replaceAll("\\", "\\\\").replaceAll('"', '\\"')}"`;
}

function shellString(value) {
  return `'${value.replaceAll("'", `'\\''`)}'`;
}

function isWithin(path, parent) {
  const absolute = resolve(path);
  const root = resolve(parent);
  return absolute === root || absolute.startsWith(`${root}/`);
}

function denyReadDataExcept(root, allowedPaths, includeAncestors = true) {
  const absoluteRoot = resolve(root);
  const exceptions = [];
  for (const allowedPath of allowedPaths) {
    if (!allowedPath || !isWithin(allowedPath, absoluteRoot)) continue;
    let current = resolve(allowedPath);
    exceptions.push(
      `(require-not (subpath ${seatbeltString(current)}))`,
      `(require-not (literal ${seatbeltString(current)}))`,
    );
    if (!includeAncestors) continue;
    while (current !== absoluteRoot) {
      current = dirname(current);
      exceptions.push(
        `(require-not (literal ${seatbeltString(current)}))`,
      );
    }
  }
  return `(deny file-read-data (require-all (subpath ${seatbeltString(absoluteRoot)}) ${[...new Set(exceptions)].join(" ")}))`;
}

function processIsolationRules(allowSelfControl = false) {
  const selfOperations = [
    "process-info-pidinfo",
    "process-info-pidfdinfo",
    "process-info-dirtycontrol",
    ...(allowSelfControl ? ["process-info-setcontrol"] : []),
  ].join(" ");
  return [
    "(deny process-info*)",
    `(allow ${selfOperations} (target self))`,
    "(deny signal)",
    "(allow signal (target self) (target children) (target same-sandbox))",
    "(deny appleevent-send)",
    '(deny mach-lookup (global-name "com.apple.pboard"))',
    '(deny mach-lookup (global-name "com.apple.pasteboard.1"))',
  ];
}

function toolchainReadablePaths(cwd, extra = []) {
  const worktreeRoot = gitText(["rev-parse", "--show-toplevel"], cwd);
  const goEnvironment = run(
    "go",
    ["env", "GOMODCACHE", "GOPATH", "GOROOT"],
    { cwd },
  ).stdout
    .trim()
    .split("\n");
  const [goModCache, , goRoot] = goEnvironment;
  return {
    goModCache,
    goRoot,
    worktreeRoot,
    readable: [
      cwd,
      worktreeRoot,
      join(repo, ".git"),
      join(repo, "web", "node_modules"),
      goModCache,
      goRoot,
      "/System",
      "/usr",
      "/bin",
      "/sbin",
      "/opt/homebrew",
      "/Library/Developer",
      "/Applications/Xcode.app",
      "/private/etc",
      "/private/var/db",
      "/dev",
      ...extra,
    ].filter((path) => path && existsSync(path)),
  };
}

function resolveAppleToolchainEnvironment(cwd) {
  if (appleToolchainEnvironment) return appleToolchainEnvironment;
  const lookup = (args) =>
    run("/usr/bin/xcrun", args, { cwd }).stdout.trim();
  appleToolchainEnvironment = {
    CC: lookup(["--find", "clang"]),
    CXX: lookup(["--find", "clang++"]),
    SDKROOT: lookup(["--show-sdk-path"]),
  };
  for (const [name, path] of Object.entries(appleToolchainEnvironment)) {
    if (!path || !existsSync(path)) {
      fail(`Could not resolve the Apple toolchain path for ${name}`);
    }
  }
  return appleToolchainEnvironment;
}

function checkSandbox(cwd, checkRoot) {
  if (process.platform !== "darwin") {
    fail(
      `No supported no-network check sandbox is configured for ${process.platform}`,
    );
  }
  const { goModCache, goRoot, readable, worktreeRoot } =
    toolchainReadablePaths(cwd, [checkRoot]);
  const filters = readable
    .map((path) => `(subpath ${seatbeltString(resolve(path))})`)
    .join(" ");
  const userHome = resolve(process.env.HOME || "/nonexistent");
  const profile = [
    "(version 1)",
    "(allow default)",
    "(deny network*)",
    '(allow network-inbound (local ip "localhost:*"))',
    '(allow network-outbound (remote ip "localhost:*"))',
    denyReadDataExcept(userHome, readable),
    denyReadDataExcept(swarmHome, [worktreeRoot, cwd, checkRoot]),
    "(deny file-write*)",
    `(allow file-read* ${filters})`,
    `(allow file-write* (literal "/dev/null") (subpath ${seatbeltString(resolve(cwd))}) (subpath ${seatbeltString(resolve(checkRoot))}))`,
    ...processIsolationRules(),
  ].join("\n");
  const home = join(checkRoot, "home");
  const temp = join(checkRoot, "tmp");
  const goCache = join(checkRoot, "go-cache");
  const goPath = join(checkRoot, "go-path");
  const npmCache = join(checkRoot, "npm-cache");
  const nextFontMocks = join(checkRoot, "next-font-google-mocks.cjs");
  for (const path of [home, temp, goCache, goPath, npmCache]) {
    mkdirSync(path, { recursive: true });
  }
  writeFileSync(
    nextFontMocks,
    `module.exports = ${JSON.stringify({
      "https://fonts.googleapis.com/css2?family=Outfit:wght@100..900&display=swap":
        "@font-face { font-family: 'Outfit'; font-style: normal; font-weight: 100 900; src: local('Arial'); }",
      "https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@200..800&display=swap":
        "@font-face { font-family: 'Plus Jakarta Sans'; font-style: normal; font-weight: 200 800; src: local('Arial'); }",
    })};\n`,
    { mode: 0o600 },
  );
  const env = {
    PATH: safeCommandPath,
    HOME: home,
    USER: process.env.USER || "nagare-swarm",
    LOGNAME: process.env.LOGNAME || process.env.USER || "nagare-swarm",
    SHELL: "/bin/sh",
    TMPDIR: temp,
    LANG: process.env.LANG || "C.UTF-8",
    LC_ALL: process.env.LC_ALL || "",
    TERM: process.env.TERM || "dumb",
    CI: "true",
    NO_COLOR: "1",
    GOCACHE: goCache,
    GOMODCACHE: goModCache,
    GOPATH: goPath,
    GOROOT: goRoot,
    npm_config_cache: npmCache,
    NPM_CONFIG_USERCONFIG: join(home, ".npmrc"),
    NEXT_FONT_GOOGLE_MOCKED_RESPONSES: nextFontMocks,
    NEXT_TELEMETRY_DISABLED: "1",
    xcrun_nocache: "1",
    ...resolveAppleToolchainEnvironment(cwd),
  };
  return {
    command: "/usr/bin/sandbox-exec",
    args: ["-p", profile, "/bin/sh", "-c"],
    env,
  };
}

function changeFingerprint(cwd) {
  const hash = createHash("sha256");
  hash.update(git(["diff", "--binary", "HEAD"], cwd).stdout);
  for (const path of changedPaths(cwd).sort()) {
    hash.update(path);
    const absolute = join(cwd, path);
    if (!existsSync(absolute)) {
      hash.update("<missing>");
      continue;
    }
    const entry = lstatSync(absolute);
    if (entry.isSymbolicLink()) {
      hash.update(`<symlink>${readlinkSync(absolute)}`);
    } else if (entry.isFile()) {
      hash.update(readFileSync(absolute));
    } else {
      hash.update(`<${entry.mode}>`);
    }
  }
  return hash.digest("hex");
}

async function runCheck(command, cwd, allowFailure = false) {
  log(`sandboxed check: ${command}`);
  const originalCwd = cwd;
  const directoryPrefix = command.match(
    /^cd ([A-Za-z0-9._/-]+) && ([\s\S]+)$/,
  );
  if (directoryPrefix) {
    if (!isSafeRelativePath(directoryPrefix[1])) {
      fail(`Check uses an unsafe working directory: ${command}`);
    }
    cwd = join(cwd, directoryPrefix[1]);
    command = directoryPrefix[2];
    if (!existsSync(cwd) || !statSync(cwd).isDirectory()) {
      fail(`Check working directory does not exist: ${cwd}`);
    }
  }
  const checkRootParent = join(swarmHome, "check-tmp");
  mkdirSync(checkRootParent, { recursive: true });
  const checkRoot = mkdtempSync(join(checkRootParent, "check-"));
  const sandbox = checkSandbox(cwd, checkRoot);
  const before = changeFingerprint(originalCwd);
  let code;
  try {
    code = await new Promise((resolvePromise, rejectPromise) => {
      const child = spawn(sandbox.command, [...sandbox.args, command], {
        cwd,
        env: sandbox.env,
        stdio: "inherit",
        detached: process.platform !== "win32",
      });
      let settled = false;
      child.on("error", async (error) => {
        if (settled) return;
        settled = true;
        try {
          await terminateProcessGroup(child.pid);
        } catch (cleanupError) {
          error.cause = cleanupError;
        }
        rejectPromise(error);
      });
      child.on("exit", async (exitCode) => {
        if (settled) return;
        settled = true;
        try {
          await terminateProcessGroup(child.pid);
          resolvePromise(exitCode);
        } catch (error) {
          rejectPromise(error);
        }
      });
    });
  } finally {
    rmSync(checkRoot, { recursive: true, force: true });
  }
  const after = changeFingerprint(originalCwd);
  if (after !== before) {
    const error = new Error(
      `Check mutated git-visible files and was rejected: ${command}`,
    );
    error.code = "CHECK_MUTATION";
    throw error;
  }
  if (code !== 0 && !allowFailure) {
    fail(`Check failed (${code}): ${command}`);
  }
  return code;
}

async function requireBaselineFailure(checks, cwd) {
  let failed = false;
  for (const check of checks) {
    if ((await runCheck(check, cwd, true)) !== 0) failed = true;
  }
  if (!failed) {
    fail(
      "Evaluator-authored tests did not make any targeted check fail against the baseline",
    );
  }
}

async function requireChecks(checks, cwd) {
  for (const check of checks) await runCheck(check, cwd);
}

function commitAll(cwd, message) {
  git(["add", "--all"], cwd);
  git(
    [
      "-c",
      "user.name=Nagare Swarm",
      "-c",
      "user.email=nagare-swarm@local",
      "commit",
      "-m",
      message,
    ],
    cwd,
  );
  return gitText(["rev-parse", "HEAD"], cwd);
}

function taskPrompt(task, plan, frozenPaths = []) {
  return `
You are the implementation agent for one bounded Nagare architecture task.

Task: ${task.title} (${task.id})
Objective: ${task.objective}
Architecture: ${task.architecture}

Acceptance criteria:
${task.acceptance.map((item) => `- ${item}`).join("\n")}

Non-goals:
${task.nonGoals.map((item) => `- ${item}`).join("\n")}

Owned paths:
${task.ownedPaths.map((item) => `- ${item}`).join("\n")}

Targeted checks:
${task.checks.map((item) => `- ${item}`).join("\n")}

Planner result:
${JSON.stringify(plan, null, 2)}

Frozen tests (never edit, move, or delete):
${frozenPaths.map((item) => `- ${item}`).join("\n")}

Implement the smallest complete solution. Do not change dependency manifests, git state, frozen tests, or files outside the owned paths. Do not commit, push, tag, publish, or spawn other agents. Return the required JSON summary.
`.trim();
}

async function executeTask(taskID) {
  const claim = claimTask(taskID);
  if (claim.skip) {
    log(`${taskID}: already ${claim.status}`);
    return;
  }
  const { initial, task, attempt, baseCommit, lease } = claim;
  let worktree;

  try {
    worktree = makeWorktree(initial.runID, taskID, baseCommit, attempt);
    updateClaimedTask(taskID, lease.owner, (taskState) => {
      taskState.worktree = worktree;
    });

    const artifacts = join(
      runsDir,
      initial.runID,
      "artifacts",
      `${taskID}-attempt-${attempt}`,
    );
    mkdirSync(artifacts, { recursive: true });

    const baselineTracked = trackedPaths(worktree);
    const baselineTestPaths = baselineTracked.filter(
      (path) => isTestFile(path) && pathIsOwned(path, task.ownedPaths),
    );
    const baselineFrozen = freezeTests(worktree, baselineTestPaths);
    await requireChecks(task.checks, worktree);

    const planner = await runCodex({
      role: "planner",
      cwd: worktree,
      prompt: `
Plan this architecture task without editing files.

Task: ${task.title}
Objective: ${task.objective}
Architecture direction: ${task.architecture}
Acceptance criteria:
${task.acceptance.map((item) => `- ${item}`).join("\n")}
Non-goals:
${task.nonGoals.map((item) => `- ${item}`).join("\n")}
Owned paths:
${task.ownedPaths.map((item) => `- ${item}`).join("\n")}
Required checks:
${task.checks.map((item) => `- ${item}`).join("\n")}

Inspect the current code, identify the narrowest viable seam, and return the required JSON plan. Do not edit, commit, or spawn other agents.
`.trim(),
      schema: join(schemaDir, "planner.schema.json"),
      artifact: join(artifacts, "planner.json"),
      sandbox: "read-only",
      task,
      frozenTests: baselineFrozen,
    });

    await runCodex({
      role: "eval-author",
      cwd: worktree,
      prompt: `
Add new behavioral tests for this architecture task. You may create test files only; do not modify any tracked file.

Task: ${task.title}
Objective: ${task.objective}
Acceptance criteria:
${task.acceptance.map((item) => `- ${item}`).join("\n")}
Owned paths:
${task.ownedPaths.map((item) => `- ${item}`).join("\n")}
Targeted checks:
${task.checks.map((item) => `- ${item}`).join("\n")}
Planner:
${JSON.stringify(planner, null, 2)}

The new tests must express the intended boundary behavior and must cause at least one targeted check to fail on the current baseline for the intended reason. Do not edit production code, existing tests, dependency manifests, or git state. Do not commit or spawn other agents. Return the required JSON summary.
`.trim(),
      schema: join(schemaDir, "agent.schema.json"),
      artifact: join(artifacts, "eval-author.json"),
      sandbox: "workspace-write",
      task,
      frozenTests: baselineFrozen,
    });

    const evalChanges = validateScope(worktree, task, baselineFrozen);
    if (evalChanges.length === 0) fail(`${taskID}: evaluator added no tests`);
    const invalidEvalChanges = evalChanges.filter(
      (path) => baselineTracked.includes(path) || !isTestFile(path),
    );
    if (invalidEvalChanges.length > 0) {
      fail(
        `${taskID}: evaluator may only add new test files: ${invalidEvalChanges.join(", ")}`,
      );
    }
    const evalCommit = commitAll(
      worktree,
      `test(${taskID}): add architecture acceptance evals`,
    );
    await requireBaselineFailure(task.checks, worktree);

    const evalFrozen = freezeTests(worktree, evalChanges);
    const allFrozen = { ...baselineFrozen, ...evalFrozen };
    let implementation = await runCodex({
      role: "implementer",
      cwd: worktree,
      prompt: taskPrompt(task, planner, Object.keys(allFrozen)),
      schema: join(schemaDir, "agent.schema.json"),
      artifact: join(artifacts, "implementation.json"),
      sandbox: "workspace-write",
      task,
      frozenTests: allFrozen,
    });

    for (let repair = 0; ; repair += 1) {
      validateScope(worktree, task, allFrozen);
      let checkError = null;
      try {
        await requireChecks(task.checks, worktree);
        validateScope(worktree, task, allFrozen);
      } catch (error) {
        if (error.code === "CHECK_MUTATION") throw error;
        checkError = error;
      }

      let review = {
        verdict: "FAIL",
        summary: "Targeted checks failed before review.",
        blockingIssues: [checkError?.message || "Unknown targeted-check failure"],
        checks: task.checks,
      };
      if (!checkError) {
        review = await runCodex({
          role: `reviewer-${repair + 1}`,
          cwd: worktree,
          prompt: `
Review the current diff from ${baseCommit} for task ${task.id}. Do not edit files.

Objective: ${task.objective}
Architecture: ${task.architecture}
Acceptance criteria:
${task.acceptance.map((item) => `- ${item}`).join("\n")}
Non-goals:
${task.nonGoals.map((item) => `- ${item}`).join("\n")}
Owned paths:
${task.ownedPaths.map((item) => `- ${item}`).join("\n")}
Frozen tests:
${Object.keys(allFrozen).map((item) => `- ${item}`).join("\n")}
Checks already passed:
${task.checks.map((item) => `- ${item}`).join("\n")}
Implementation report:
${JSON.stringify(implementation, null, 2)}

Return PASS only if the implementation satisfies the task with no blocking correctness, regression, concurrency, scope, or test-integrity findings. Return the required JSON object.
`.trim(),
          schema: join(schemaDir, "reviewer.schema.json"),
          artifact: join(artifacts, `review-${repair + 1}.json`),
          sandbox: "read-only",
          task,
          frozenTests: allFrozen,
        });
      }

      if (review.verdict === "PASS") break;
      if (repair >= initial.maxRepairAttempts) {
        fail(
          `${taskID}: review/check gate still failing after ${initial.maxRepairAttempts} repairs: ${review.blockingIssues.join("; ")}`,
        );
      }
      implementation = await runCodex({
        role: `repair-${repair + 1}`,
        cwd: worktree,
        prompt: `
Repair the bounded implementation for ${task.id}.

Blocking findings:
${review.blockingIssues.map((item) => `- ${item}`).join("\n")}

${taskPrompt(task, planner, Object.keys(allFrozen))}

Address only the blocking findings. Do not broaden scope, edit frozen tests, change dependencies, or modify git state.
`.trim(),
        schema: join(schemaDir, "agent.schema.json"),
        artifact: join(artifacts, `repair-${repair + 1}.json`),
        sandbox: "workspace-write",
        task,
        frozenTests: allFrozen,
      });
    }

    const implementationChanges = validateScope(worktree, task, allFrozen);
    if (implementationChanges.length === 0) {
      fail(`${taskID}: implementation produced no changes`);
    }
    await requireChecks(task.checks, worktree);
    validateScope(worktree, task, allFrozen);
    const implementationCommit = commitAll(
      worktree,
      `refactor(${taskID}): implement architecture boundary`,
    );
    updateClaimedTask(taskID, lease.owner, (taskState) => {
      taskState.status = "completed";
      taskState.evalCommit = evalCommit;
      taskState.implementationCommit = implementationCommit;
      taskState.error = null;
      taskState.lease = null;
    });
    log(`${taskID}: completed at ${implementationCommit}`);
  } catch (error) {
    try {
      updateClaimedTask(taskID, lease.owner, (taskState) => {
        taskState.status = "failed";
        taskState.error = error.message;
        taskState.lease = null;
        if (worktree) taskState.worktree = worktree;
      });
    } catch (stateError) {
      log(`${taskID}: could not record failure: ${stateError.message}`);
    }
    throw error;
  }
}

async function integrateWave(wave) {
  const claim = claimIntegration(wave);
  if (claim.skip) {
    log(`wave ${wave}: already integrated`);
    return;
  }
  const { initial, tasks, lease } = claim;

  const attempt = readdirSync(join(worktreesDir, initial.runID), {
    withFileTypes: true,
  }).filter((entry) => entry.name.startsWith(`integration-wave-${wave}-`))
    .length + 1;
  const worktree = makeWorktree(
    initial.runID,
    `integration-wave-${wave}`,
    initial.integrationHead,
    attempt,
  );
  try {
    for (const task of tasks) {
      const taskState = initial.tasks[task.id];
      for (const commit of [
        taskState.evalCommit,
        taskState.implementationCommit,
      ]) {
        log(`wave ${wave}: cherry-picking ${task.id} ${commit}`);
        git(["cherry-pick", commit], worktree);
      }
    }
    git(["diff", "--check", `${initial.integrationHead}..HEAD`], worktree);
    await requireChecks(initial.globalChecks, worktree);
    if (changedPaths(worktree).length > 0) {
      fail(`wave ${wave}: checks left git-visible integration changes`);
    }
    const head = gitText(["rev-parse", "HEAD"], worktree);
    updateClaimedIntegration(wave, lease.owner, (state) => {
      state.integrationHead = head;
      state.integratedWaves.push(wave);
      state.integratedWaves.sort((a, b) => a - b);
      for (const task of tasks) state.tasks[task.id].status = "integrated";
      state.lastIntegrationWorktree = worktree;
      state.integrationError = null;
      delete state.integrationLeases[wave];
    });
    log(`wave ${wave}: integrated at ${head}`);
  } catch (error) {
    try {
      updateClaimedIntegration(wave, lease.owner, (state) => {
        state.integrationError = {
          wave,
          worktree,
          message: error.message,
        };
        delete state.integrationLeases[wave];
      });
    } catch (stateError) {
      log(`wave ${wave}: could not record failure: ${stateError.message}`);
    }
    throw error;
  }
}

function finalize() {
  const claim = claimFinalization();
  if (claim.skip) {
    log(`Already finalized at ${claim.branch}`);
    return;
  }
  const { initial, lease } = claim;
  const branch = `codex/architecture-swarm-${initial.runID.toLowerCase()}`;
  const existing = git(
    ["rev-parse", "--verify", `refs/heads/${branch}`],
    repo,
    { allowFailure: true },
  );
  if (existing.status === 0) {
    if (existing.stdout.trim() !== initial.integrationHead) {
      fail(`Local branch ${branch} already exists at a different commit`);
    }
  } else {
    git(["branch", branch, initial.integrationHead]);
  }
  updateState((state) => {
    if (state.finalizeLease?.owner !== lease.owner) {
      fail("finalize: lease ownership was lost");
    }
    state.status = "completed";
    state.branch = branch;
    state.completedAt = new Date().toISOString();
    state.finalizeLease = null;
  });
  log(`Finalized local branch ${branch} at ${initial.integrationHead}`);
  log("No push, pull request, tag, deployment, or primary-checkout change occurred.");
}

function status() {
  if (!existsSync(activeRunPath)) {
    process.stdout.write(
      `${JSON.stringify({ status: "idle", activeRun: null }, null, 2)}\n`,
    );
    return;
  }
  const state = readState();
  process.stdout.write(
    `${JSON.stringify(
      {
        runID: state.runID,
        status: state.status,
        baseCommit: state.baseCommit,
        integrationHead: state.integrationHead,
        integratedWaves: state.integratedWaves,
        branch: state.branch,
        tasks: state.tasks,
        integrationError: state.integrationError || null,
      },
      null,
      2,
    )}\n`,
  );
}

async function main() {
  const [command, argument, ...flags] = process.argv.slice(2);
  switch (command) {
    case "validate": {
      const backlog = validateRuntime();
      if (argument === "--ready" || flags.includes("--ready")) preflightReady();
      log(
        `Validated ${backlog.tasks.length} tasks across ${new Set(backlog.tasks.map((task) => task.wave)).size} waves`,
      );
      break;
    }
    case "create-manifest":
      createManifest(argument, flags[0]);
      break;
    case "sandbox-smoke":
      await runCheck("git diff --check", repo);
      await runCheck("! ps -p 1 >/dev/null 2>&1", repo);
      if (
        process.env.HOME &&
        existsSync(join(process.env.HOME, ".codex", "auth.json"))
      ) {
        await runCheck(
          `! head -c 1 ${shellString(join(process.env.HOME, ".codex", "auth.json"))} >/dev/null 2>&1`,
          repo,
        );
      }
      if (
        process.env.HOME &&
        existsSync(join(process.env.HOME, ".zsh_history"))
      ) {
        await runCheck(
          `! head -c 1 ${shellString(join(process.env.HOME, ".zsh_history"))} >/dev/null 2>&1`,
          repo,
        );
      }
      if (existsSync(join(swarmHome, "api-key"))) {
        await runCheck(
          `! head -c 1 ${shellString(join(swarmHome, "api-key"))} >/dev/null 2>&1`,
          repo,
        );
      }
      if (process.env.SWARM_EXPECT_CLIPBOARD_SECRET === "1") {
        await runCheck("! pbpaste >/dev/null 2>&1", repo);
      }
      {
        const daemonPIDPath = join(swarmHome, "nagare.pid");
        if (existsSync(daemonPIDPath)) {
          const daemonPID = Number.parseInt(
            readFileSync(daemonPIDPath, "utf8").trim(),
            10,
          );
          if (processIsAlive(daemonPID)) {
            await runCheck(
              `! kill -0 ${daemonPID} >/dev/null 2>&1`,
              repo,
            );
          }
        }
      }
      {
        mkdirSync(worktreesDir, { recursive: true });
        const nestedWorktree = join(
          worktreesDir,
          `.sandbox-smoke-${process.pid}-${randomUUID()}`,
        );
        try {
          git(["worktree", "add", "--detach", nestedWorktree, "HEAD"], repo);
          const trackedPath = "web/src/app/dags/page.tsx";
          const trackedAbsolute = join(nestedWorktree, trackedPath);
          writeFileSync(
            trackedAbsolute,
            `${readFileSync(trackedAbsolute, "utf8")}\n`,
          );
          const observedPaths = changedPaths(nestedWorktree);
          if (
            observedPaths.length !== 1 ||
            observedPaths[0] !== trackedPath
          ) {
            fail(
              `Git scope parser returned ${JSON.stringify(observedPaths)} instead of ${trackedPath}`,
            );
          }
          await runCheck(
            "cd web && test -r ../package.json",
            nestedWorktree,
          );
        } finally {
          if (existsSync(nestedWorktree)) {
            git(["worktree", "remove", "--force", nestedWorktree], repo);
          }
          git(["worktree", "prune"], repo);
        }
      }
      {
        const descendantMarker = join(
          repo,
          `.nagare-swarm-descendant-smoke-${process.pid}`,
        );
        if (existsSync(descendantMarker)) {
          fail(`Descendant smoke marker already exists: ${descendantMarker}`);
        }
        try {
          await runCheck(
            `(sleep 1; touch ${shellString(descendantMarker)}) &`,
            repo,
          );
          await new Promise((resolvePromise) =>
            setTimeout(resolvePromise, 1_250),
          );
          if (existsSync(descendantMarker)) {
            fail("A sandboxed background descendant escaped process-group cleanup");
          }
        } finally {
          rmSync(descendantMarker, { force: true });
        }
      }
      await runCheck("CGO_ENABLED=1 go test ./internal/...", repo);
      await runCheck("cd web && npm run test:ci && npm run build", repo);
      log("Validated the no-network check sandbox");
      break;
    case "init-run":
      initializeRun();
      break;
    case "prepare-trigger":
      prepareTrigger();
      break;
    case "execute-task":
      if (!argument) fail("Usage: swarmctl.mjs execute-task <task-id>");
      await executeTask(argument);
      break;
    case "integrate-wave": {
      const wave = Number.parseInt(argument, 10);
      if (!Number.isInteger(wave)) {
        fail("Usage: swarmctl.mjs integrate-wave <wave-number>");
      }
      await integrateWave(wave);
      break;
    }
    case "finalize":
      finalize();
      break;
    case "status":
      status();
      break;
    default:
      fail(
        "Usage: swarmctl.mjs <validate [--ready]|sandbox-smoke|create-manifest SHA DIRTY_COUNT|prepare-trigger|init-run|execute-task ID|integrate-wave N|finalize|status>",
      );
  }
}

main().catch((error) => {
  process.stderr.write(`[swarm] ERROR: ${error.stack || error.message}\n`);
  process.exitCode = 1;
});
