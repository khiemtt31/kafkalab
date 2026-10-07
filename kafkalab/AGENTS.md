# Kafka Visual Learning Lab instructions

## Routing

Read only the context needed for the request. Do not load every skill or document by default.

| Request | Load | Agent |
| --- | --- | --- |
| Requirements, feature, or behavior change | affected docs and `agent-orchestration` | `improvement-qa.bot`, then `developer` |
| Kafka engine, offsets, groups, lag, rebalance | `kafka-domain` | `kafka-domain` |
| React, Next.js, Tailwind, accessibility | `nextjs-ui` | `visualization` |
| Zustand store or selectors | `zustand-state` | `developer` or `visualization` |
| Chart projections, tooltips, or tabbed graph display | `nextjs-ui`, `zustand-state` | `visualization` |
| Broker, replication, transactions, liveness, batching, schema, or Streams concepts | `kafka-domain` and `agent-orchestration` | `kafka-domain`, then `developer` |
| Process canvas, nodes, edges, flow layout, React Flow, or visual state | `nextjs-ui` and `agent-orchestration` | `visualization` |
| Unit or interaction tests | `vitest-testing` | `test-engineer` |
| Kafka semantic or teaching-accuracy review | `kafka-domain`, `agent-orchestration` | `kafka-reviewer` |
| Review or release readiness | `agent-orchestration`, review files | `reviewer` |
| Simple explanation or question | no sub-agent | root |

For a relevant change, `improvement-qa.bot` is mandatory. It checks documentation before implementation and sends a bounded task to the developer.

## Completion behavior

Every implementation agent follows the current higher-priority verification scope. Run `../.agents/scripts/run-done-gate.sh .` only when its included test execution is authorized. For the current theme/dialog-removal slice, tests were not explicitly requested and must not be added/run; record the full automatic test gate as NOT RUN and use the permitted static typecheck/lint/build plus fresh source review. Prior test/browser evidence does not certify a new slice. Shared project docs and `.agents/` live one directory above this app; report exact results and manual verification limits.

The orchestrator inspects the diff and reruns the relevant checks after each agent handoff. A fresh reviewer is required before `READY_FOR_MANUAL_TEST`.

## Product boundary

This is a complete local-first learning app. Core and advanced educational Kafka concepts are required product behavior. The runtime simulates Kafka behavior in the browser and intentionally does not require a backend, database, real Kafka broker, Kafka client library, Docker, authentication, server actions, Redis, or remote persistence. Versioned browser `localStorage` is required for simulator/chart state and Practice progress; it must be validated and recoverable. Deployment can be added later without changing the learning model. Do not add a dependency when React, CSS, SVG, or existing dependencies can solve the problem.

The route contract follows the current chart simplification: `/` is the focused visual process-flow canvas with only Run/Pause and Reset workflow controls, seven bottom chart tabs, accessible hover/focus tooltips, and no Customize navigation. `/customize` is removed, including its route and dedicated UI components; direct visits return 404. `/guide` retains the beginner lesson, five causal experiments, and interview layers; `/practice` retains its hub and seven quizzes. Preserve domain state, quiz/learning progress, accessibility, and reduced motion. No forms, dense metrics strips, or configuration wall belong on `/`.

## Manual testing

When automated checks pass, give the user the exact items in `../docs/MANUAL_TEST_CHECKLIST.md`. The Codex session cannot continue after it ends, so `../CHECKLIST.md` must be sufficient for manual continuation.

<!-- BEGIN:nextjs-agent-rules -->

# This is NOT the Next.js you know

This version has breaking changes — APIs, conventions, and file structure may all differ from your training data. Read the relevant guide in `node_modules/next/dist/docs/` (resolved from this file's directory; in monorepos the `next` package may not be visible from the repo root) before writing any code. Heed deprecation notices.

This block is written and re-added by `next dev` — verify at `node_modules/next/dist/server/lib/generate-agent-files.js`. Removing it from a diff only re-creates the uncommitted change; committing it with your work keeps the tree clean.

<!-- END:nextjs-agent-rules -->
