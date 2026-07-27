---
name: run-nagare-swarm
description: Operate the repository's guarded, unattended Nagare/Codex architecture swarm. Use when the user asks to start, resume, monitor, cancel, diagnose, or update the approved divide-and-conquer refactor loop, or explicitly mentions the Nagare swarm.
---

# Run Nagare Swarm

Use the pinned control plane through `scripts/nagare-swarm`. Never substitute `go run`, the development checkout's `./nagare`, or multiple write agents in one checkout.

Read [references/operations.md](references/operations.md) before operating or recovering a run.

## Workflow

1. Run `scripts/nagare-swarm status`.
2. For setup or a new pinned release, build an exact Nagare binary from the intended commit, then run `scripts/nagare-swarm install --binary ABSOLUTE_PATH`.
3. Before execution, require the control files to be committed and tracked. Run `scripts/nagare-swarm validate --ready`.
4. Start the scheduler with `scripts/nagare-swarm start`.
5. Trigger once with `scripts/nagare-swarm trigger`. A retrigger resumes the active state and short-circuits completed stages.
6. Authenticate the UI before triggering, then monitor at `http://127.0.0.1:18080`; the API key is sealed after launch. Use `status` for local runner state. Keep working until the run either creates its local result branch or reaches a concrete failed gate.
7. Do not push, open a pull request, tag, deploy, merge, or switch the primary checkout unless the user separately asks.

## Enforcement boundary

The runner is authoritative. Every implementation task gets:

- a detached worktree and explicit owned paths;
- independent planner, evaluator, implementer, and reviewer sessions;
- new evaluator-authored tests that must fail on the baseline;
- immutable existing and evaluator-authored tests during implementation;
- targeted checks, read-only review, and at most two repair rounds;
- local commits made by the runner, then wave-by-wave cherry-pick integration;
- full repository checks before progression.

Treat project hooks as early feedback only. Never use approval/sandbox or hook-trust bypass flags.

If a gate fails, preserve its worktree and artifacts. Report the failing task, check, worktree, and safe recovery action; do not silently weaken the gate.
