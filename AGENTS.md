# Nagare contributor instructions

## Repository checks

- Go tests: `make test`
- Full lint: `make lint`
- Production build: `make build`
- Frontend tests: `cd web && npm run test:ci`
- Frontend formatting: `cd web && npm run format:check`

Keep changes focused, preserve public behavior unless the task explicitly changes it, and prefer small modules with narrow interfaces. Add tests at the closest useful boundary.

## Unattended swarm boundary

When a task is running under `SWARM_TASK_ID`:

- Work only inside the owned paths named in the prompt.
- Do not edit tests marked as frozen.
- Do not add or update dependencies unless the prompt explicitly allows it.
- Do not commit, push, tag, publish, merge, or modify the primary checkout.
- Do not weaken, delete, or skip tests to make a check pass.
- Treat `.codex/hooks/swarm-stop-check.mjs` as an early warning only; the external swarm runner is the authoritative gate.

When asked to use `$run-nagare-swarm`, use the pinned Nagare control plane through `scripts/nagare-swarm`. Do not substitute `go run`, the checkout's `./nagare`, or several writers in the same checkout.
