# Agentic Workflows Product Plan

## Outcome

Evolve Nagare from a lean DAG engine into a local-first control plane for durable, guarded workflows coordinating people, tools, and AI agents, without sacrificing its single-binary operation or binding the core to any model or workplace vendor.

A user should be able to install or author a Workflow Pack, bind their context sources, models, approval channel, delivery targets, and policy, then let Nagare continuously discover, propose, execute, evaluate, and deliver work within explicit capabilities.

The product promise is:

> Define the work process once. Nagare coordinates the agents, tools, approvals, evidence, and delivery.

## Product constraints

- Preserve the current command, container, map, trigger, pool, retry, cancellation, cluster, and observability behavior.
- Keep Nagare Core model-, prompt-, connector-, and deliverable-agnostic.
- Retain a useful zero-external-dependency single-node mode backed by SQLite.
- Keep every external effect attributable, idempotent where possible, and bounded by policy.
- Do not require a human to prompt each Agent or wait inside a running Task for human input.
- Treat external text, repository content, and documents as untrusted data rather than instructions.
- Make all generated plans, requested capabilities, approvals, artifacts, evaluations, and receipts inspectable.
- Add abstractions only after at least two real adapters or workflows prove the seam.

## Architecture

The decision in [ADR 0001](adr/0001-agentic-workflows-above-the-core.md) creates three product layers.

### Nagare Core

Core owns durable orchestration:

- Workflow and Run lifecycle
- Task Attempt lifecycle
- Structured Run inputs
- Typed Artifact persistence and references
- Durable external events and waiting
- Conditional outcomes and skipped Tasks
- Nested Workflow input and output contracts
- Secret references and declared capabilities
- Scheduling, retries, cancellation, pools, workers, logs, metrics, and inspection

Core must not know about model names, prompts, Jira, Slack, GitHub, Telegram, pull requests, or agent roles.

### Agent Kit

The optional Agent Kit owns reusable agent execution behavior:

- Context bundle construction
- Assignment validation
- Role and model routing
- Planner-driven fan-out
- Isolated worktrees or containers
- Evaluator-authored acceptance tests
- Independent implementation review
- Bounded repair loops
- Integration of parallel results
- Agent cost, token, time, and attempt budgets
- Provenance for model, context, tools, and generated Artifacts

An Agent adapter receives one Assignment and returns an Artifact set. The adapter may use Codex, another model, a local model, or a human-backed implementation without changing Workflow semantics.

### Workflow Packs

A Workflow Pack is the reusable authoring and distribution layer. A pack contains:

```text
pack.yaml
dags/
roles/
policies/
schemas/
evals/
ui/
```

The pack manifest declares:

- Version and compatible Nagare versions
- Inputs and outputs
- Adapter bindings
- Required secrets
- Requested capabilities
- Included Workflows and assets
- Policy defaults and configurable limits
- Content digests

Packs render to inspectable Nagare Workflows and pinned assets. Installing or upgrading a pack must display capability changes and must not silently broaden access.

## Target lifecycle

```text
Signal
  -> Work Item
  -> Proposal
  -> Approval or policy grant
  -> Plan
  -> Assignments
  -> Artifacts
  -> Evaluations
  -> Delivery approval or policy grant
  -> Delivery
  -> Receipt
```

Rejection, expiry, failure, cancellation, and supersession are explicit terminal or retryable outcomes. A Run is an execution attempt supporting this lifecycle; the Run database is not the authoritative Work Ledger.

## Milestones

Each milestone should ship as a focused pull request or a short series of stacked pull requests. Each production change starts with deterministic module-level acceptance tests, followed by the narrow implementation and compatibility adapters.

### Milestone 0: Freeze the product contracts

Deliverables:

- Adopt the language in `CONTEXT.md`.
- Record the Core versus Agent Kit versus Workflow Pack decision.
- Define versioned JSON Schemas for Assignment, Artifact metadata, Evaluation, pack manifest, capability request, Approval, and Receipt.
- Provide realistic fixtures for a software change and an ML experiment.
- Document threat assumptions and supported trust levels for context.

Acceptance:

- Schemas reject missing budgets, output contracts, capability declarations, proposal digests, and idempotency keys where required.
- Fixtures validate without depending on a model provider.
- Existing Nagare behavior and checks remain unchanged.

### Milestone 1: Structured Run inputs and typed Artifacts

Deliverables:

- Add immutable structured inputs to a Run while preserving the existing string `Conf` environment projection.
- Add an Artifact module with immutable metadata, digest, media type, producer Task Attempt, optional inline payload, and external reference.
- Add Artifact query and publication interfaces at a seam shared by local and remote execution.
- Add authenticated endpoints and CLI commands for Run inputs and Artifacts.
- Extend run inspection and the UI with an Artifact timeline.
- Enforce payload and count limits so SQLite and the UI cannot be exhausted by task output.

Acceptance:

- Manual, webhook, cron, and child-Workflow Runs receive deterministic immutable inputs.
- Replaying the same Artifact publication is idempotent; a conflicting digest fails explicitly.
- A remote Task publishes the same Artifact representation as a local Task.
- Existing environment-variable workflows remain compatible.
- Restart, retry, cancellation, and concurrent publication tests pass under `go test -race`.

### Milestone 2: Durable external events and waiting

Deliverables:

- Add a durable Event module with source, kind, correlation key, idempotency key, payload reference, and observed time.
- Add a scheduler-owned waiting Task that consumes no worker slot.
- Support event-before-wait and wait-before-event ordering.
- Support cancellation, expiry, restart recovery, and exact event consumption.
- Add authenticated event ingestion and HMAC webhook adapters.
- Show waiting reason, correlation, and expiry in run inspection and the UI.

Acceptance:

- Duplicate event delivery cannot resume a Task twice.
- A matching event recorded before Task registration is consumed after registration.
- Restart cannot lose or duplicate a resume.
- Cancelling a Run makes late matching events harmless.
- Expiry produces a deterministic configured outcome.

### Milestone 3: Conditional outcomes and child-Workflow contracts

Deliverables:

- Add `skipped` as an explicit Task outcome without weakening success and failure semantics.
- Define a small conditional interface over structured inputs, Task outcomes, and Artifact metadata; do not embed a general programming language.
- Extend child-Workflow triggering with declared structured inputs and optional completion waiting.
- Publish declared child outputs as parent-visible Artifacts.
- Make progression, retry, cancellation, maps, and inspection understand skipped Tasks and child contracts.

Acceptance:

- Conditions are deterministic and fully evaluable without invoking a model.
- Skipped dependencies cannot accidentally authorize Tasks that require success.
- Parent cancellation propagates according to the declared child policy.
- Child retries and replay do not duplicate parent-visible Artifacts.
- Existing `trigger_dag` behavior remains compatible.

### Milestone 4: Workflow Pack validation and rendering

Deliverables:

- Add `nagare packs validate`, `nagare packs render`, and `nagare packs inspect`.
- Support local, pinned pack sources first; defer a remote registry until the format is proven.
- Validate pack versions, assets, schemas, adapter bindings, secrets, capabilities, and Nagare compatibility.
- Render packs into ordinary Workflow YAML and content-addressed assets.
- Produce a lockfile containing every resolved version and digest.
- Report capability differences during install or upgrade.

Acceptance:

- Rendering is deterministic and produces no hidden runtime configuration.
- Tampered assets or manifests fail before execution.
- A pack update requesting broader capabilities requires an explicit policy decision.
- Rendered Workflows pass normal Nagare parsing and validation.
- Packs can be exported and run without the authoring source tree.

### Milestone 5: Generic Agent Kit

Deliverables:

- Define the executable/container Agent adapter protocol using JSON input, JSON result, streamed logs, and explicit exit classification.
- Implement Assignment validation, role routing, budgets, stop conditions, and provenance.
- Implement isolated workspace creation with owned paths and declared capabilities.
- Implement planner, evaluator-author, implementer, reviewer, repair, and integrator roles as configuration rather than provider-specific code.
- Support validated planner fan-out with bounded parallelism.
- Provide a deterministic fake Agent adapter for tests.
- Generalize the existing guarded architecture swarm without breaking its pinned control path.

Acceptance:

- A planner cannot create more Assignments, capabilities, time, or scope than its parent Assignment grants.
- Evaluator-authored tests fail on the baseline and remain immutable during implementation.
- Parallel implementers cannot write outside owned paths.
- Only the integrator writes the integration branch.
- Reviewers receive requirements, Artifacts, and evidence without inheriting the implementer transcript.
- Repair loops stop at the configured limit and preserve failed workspaces and evidence.
- No Agent session can commit, publish, merge, tag, deploy, or access control-plane credentials unless the exact capability is granted.

### Milestone 6: Work Ledger and human approval

Deliverables:

- Add a Work Ledger module separate from Run state.
- Persist Signals, Work Items, Proposals, Approvals, Plans, Artifacts, Evaluations, Deliveries, and Receipts.
- Bind every Approval to an immutable proposal or delivery digest, actor, expiry, and granted capabilities.
- Implement a generic approval adapter interface over durable events.
- Provide a Telegram adapter as the first external channel and the Nagare UI as the second.
- Add Work Inbox, Proposal, Approval Queue, Artifact, Evaluation, and Delivery views.

Acceptance:

- Approval of a stale or changed proposal is rejected.
- Approval replay is idempotent and cannot broaden capabilities.
- Waiting for approval consumes no worker.
- The external channel never receives control-plane or model credentials.
- Every external effect can be traced to the exact Work Item, Proposal, Approval or policy grant, Artifact, and Receipt.

### Milestone 7: Reference adapters and Workflow Packs

Deliverables:

- Context source adapters for GitHub and Jira, then Slack after the source contract is proven.
- Approval adapters for the UI and Telegram.
- Delivery adapters for draft pull requests, Jira updates, reports, and notifications.
- Agent adapters for at least two backends before stabilizing the Agent adapter seam.
- `software-change` pack generalized from the architecture swarm.
- `ml-experiment` pack covering dataset validation, experiment planning, parallel training or evaluation, comparison, and a recommendation report.

Acceptance:

- Users can replace source, Agent, approval, and delivery adapters independently.
- The software-change pack completes against a fixture repository using fake external adapters.
- The ML experiment pack completes against a small fixture dataset using deterministic local commands.
- Neither pack requires provider-specific fields in Core Workflow definitions.
- External writes are disabled in fixture and dry-run modes.

### Milestone 8: Policy, security, evaluation, and operations

Deliverables:

- Capability policies at global, pack, Workflow, Task, and Assignment scopes with most-restrictive-wins semantics.
- Secret references with least-privilege delivery and redacted logs.
- Context trust labels, prompt-injection handling, payload limits, and retention policy.
- Global and per-Work-Item kill switches.
- Cost, token, model, approval latency, rejection, repair, and delivery metrics.
- Golden-set replay tooling for qualification, planning, and evaluation decisions.
- Pack signing and remote distribution only after the local pack format is stable.
- Backup, restore, migration, and upgrade documentation.

Acceptance:

- Policy cannot be weakened by a child Workflow, planner, Agent, or adapter.
- Secrets never enter persisted prompts, Artifacts, logs, or UI payloads.
- Untrusted context cannot request capabilities or change system instructions.
- Golden-set results are versioned and comparable across pack, prompt, policy, and model changes.
- A clean install can inspect, dry-run, activate, stop, resume, and audit both reference packs.

## Evaluation strategy

### Deterministic module evaluations

- State transitions, replay, idempotency, cancellation, expiry, and restart recovery
- Artifact hashing, size limits, and provenance
- Event matching and exact consumption
- Capability narrowing
- Pack rendering and lockfile integrity
- Workspace path ownership and frozen tests

### Agent evaluations

- Historical or synthetic Signals with expected qualification decisions
- Plans with expected Assignments, dependencies, capabilities, and budgets
- Known repository tasks with acceptance tests authored before implementation
- Independent review verdicts over seeded correct and incorrect Artifacts
- Prompt-injection fixtures embedded in tickets, documents, source files, and tool output

### Product metrics

- Signal-to-Work-Item precision and missed-work rate
- Proposal approval, rejection, expiry, and edit rate
- Human attention time per delivered Work Item
- Artifact acceptance and repair rate
- Pull-request merge and rework rate
- Delivery replay prevention
- Agent time, tokens, cost, and failure classification
- Security-policy violations and blocked capability escalations

Every user correction or rejected Proposal should be capturable as a new golden-set case.

## Delivery and branching strategy

- One milestone per independently reviewable pull request or short stack.
- Separate schema/tests, implementation, UI, and documentation commits when practical.
- Preserve compatibility adapters until their removal has a separately approved migration.
- Do not mix vendor adapters into Core changes.
- Do not publish packs or enable external writes from test or development runs.
- Run `make test`, `make lint`, and `make build` for every milestone, plus focused race, restart, and frontend checks for affected modules.

## First executable slice

The first product slice after the contracts are frozen is:

```text
GitHub issue fixture
  -> Work Item and Proposal
  -> UI approval
  -> generic software-change Plan
  -> evaluator-authored tests
  -> isolated parallel Assignments
  -> integration and independent review
  -> draft pull-request Artifact
  -> dry-run Delivery Receipt
```

Use fake source, Agent, approval, and delivery adapters first. Replace them one seam at a time with real adapters only after deterministic end-to-end evaluation passes.

The second slice is an ML experiment because it tests that the framework coordinates general work rather than only source-code changes.

## Definition of product completion

This initiative is complete when:

- Nagare Core exposes the required generic durable primitives without provider-specific concepts.
- Users can validate, inspect, render, pin, and run versioned Workflow Packs.
- At least two Agent backends, two approval adapters, and two materially different reference packs use the same stable seams.
- The UI explains what was observed, proposed, approved, executed, evaluated, and delivered.
- Capability, secret, idempotency, provenance, and kill-switch guards have deterministic evaluations.
- A user can adapt a reference pack to their own sources, models, policies, and delivery targets without editing Nagare Core.
