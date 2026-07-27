---
status: accepted
---

# Keep agentic workflows above the orchestration core

Nagare will become a local-first control plane for durable, guarded workflows coordinating people, tools, and AI agents, while keeping model providers, prompts, connectors, agent roles, and deliverable-specific behavior out of the orchestration core. Nagare Core will add only generic primitives such as structured run inputs, typed artifacts, durable external events, waiting, conditional outcomes, secret references, and capabilities; an optional Agent Kit and versioned Workflow Packs will compose those primitives into agentic products. This preserves the lean single-binary engine, keeps provider dependencies replaceable, and prevents the Workflow and Task interfaces from expanding for every model or workplace integration.

## Considered options

- Put models, prompts, tools, approvals, and connectors directly in Workflow task definitions. This offers a short path to a demo but permanently couples the core interface to a fast-changing ecosystem.
- Keep all agentic behavior in an unrelated external application. This protects the core but makes guarded execution, artifacts, approvals, and observability feel bolted on rather than like a Nagare product.
- Add generic orchestration primitives to Core and build the Agent Kit and Workflow Packs above them. This is the chosen design because it gives users a coherent product without making the engine provider-aware.

## Consequences

Workflow Packs compile or render into inspectable Nagare Workflows and pinned assets instead of creating a second hidden scheduler. Adapters run as executables or containers with declared schemas, capabilities, secrets, and side effects. Agent-specific dependencies must not enter the default execution path of Nagare Core.
