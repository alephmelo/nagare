# Nagare

Nagare coordinates durable workflows made of tasks, people, tools, and autonomous actors. This language distinguishes orchestration state from the work products and decisions carried through a workflow.

## Orchestration

**Workflow**:
A reusable directed graph that describes task dependencies, triggers, and execution policy.
_Avoid_: Pipeline, flow

**Run**:
One durable execution of a Workflow with fixed inputs.
_Avoid_: Job, session

**Task**:
A named step in a Workflow.
_Avoid_: Node, step

**Task Attempt**:
One execution attempt of a Task within a Run.
_Avoid_: Task instance, retry

**Worker**:
A Nagare execution process that claims and runs Task Attempts from one or more pools.
_Avoid_: Agent, executor

## Agentic Work

**Signal**:
An immutable observation from an external context source that may indicate actionable work.
_Avoid_: Message, event, notification

**Work Item**:
The authoritative record of a potential outcome Nagare may coordinate from proposal through delivery.
_Avoid_: Ticket, job, request

**Proposal**:
A versioned recommendation for how to handle a Work Item, including expected effects and required capabilities.
_Avoid_: Suggestion, prompt

**Approval**:
A time-bounded decision authorizing the exact digest of a Proposal or Delivery.
_Avoid_: Confirmation, permission

**Plan**:
A validated decomposition of an approved Proposal into Assignments and their dependencies.
_Avoid_: Agent DAG, prompt chain

**Assignment**:
A bounded objective given to one Agent, including inputs, capabilities, budget, output contract, and acceptance criteria.
_Avoid_: Prompt, subtask

**Agent**:
An autonomous actor that fulfills an Assignment through a configured model-and-tool adapter.
_Avoid_: Worker, bot

**Artifact**:
An immutable, typed result produced by a Task Attempt or Agent and referenced by digest.
_Avoid_: Output, response, file

**Evaluation**:
Evidence and a verdict about whether an Artifact satisfies named acceptance criteria and policies.
_Avoid_: Review, check

**Delivery**:
A proposed external effect that publishes or applies approved Artifacts.
_Avoid_: Action, deployment

**Receipt**:
Immutable evidence that a Delivery was applied exactly once or was definitively rejected.
_Avoid_: Result, response

**Capability**:
A named class of side effect that a Workflow Pack, Task, or Assignment may request and policy may grant.
_Avoid_: Tool, permission scope

**Context Item**:
An immutable, provenance-bearing piece of information made available to an Assignment as data.
_Avoid_: Prompt content, memory

**Trust Class**:
A policy label describing who controls a Context Item and whether it may supply instructions or only data.
_Avoid_: Trusted flag, source type

**Workflow Pack**:
A versioned, inspectable bundle of Workflow definitions, schemas, roles, policies, evaluations, and presentation metadata.
_Avoid_: Plugin, template

**Work Ledger**:
The authoritative history of Work Items, Proposals, Approvals, Plans, Artifacts, Evaluations, Deliveries, and Receipts.
_Avoid_: Agent state, run database
