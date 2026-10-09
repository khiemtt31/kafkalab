# Definition of Done

## Current Flow/Guide learning redesign scope

Follow [FLOW_GUIDE_LEARNING_REDESIGN.md](FLOW_GUIDE_LEARNING_REDESIGN.md) and manual section 17. The new question-led workbench, focused paths, visible diagnosis, and deliberate optional details supersede earlier Run/Reset-only and hover-first requirements. Meaningful tests, typecheck, lint, build, diff inspection, fresh independent review, and production HTTP smoke are engineering gates; browser visual/interaction acceptance remains separate. Exact final results are recorded in [CHECKLIST.md](../CHECKLIST.md).

Whole-system layouts persist independently per view; focused paths use temporary canonical geometry. Preserve shared simulation, first-record exercise, Practice/learning/theme independence, removed Customize/INSPECT STEP, stable bounded graph entities, and honest captured versus live evidence.

## Historical theme/dialog-removal scope

The current chart simplification removes the Customize route, visible send/reference actions, execution strip, and floating inspector surfaces. It adds seven bounded chart projections, accessible hover/focus tooltips, per-variant layout persistence, and native bottom tab semantics while preserving simulator, Guide, Practice, learning, and HANZO footer data. Automated tests are authorized for this slice; browser UI acceptance remains manual.

## Status vocabulary

`IN_PROGRESS`: work or required checks remain.

`BLOCKED`: progress cannot continue because an external dependency or unresolved decision is required; include the exact blocker.

`READY_FOR_MANUAL_TEST`: automated gates and review pass; the user must run the browser checklist.

`COMPLETE`: the requested milestone has passed automated and manual acceptance.

## Agent-level gate

An agent can finish a task only when it has:

- identified affected stories and criteria;
- updated documentation when behavior changed;
- implemented within its ownership boundary;
- added or updated tests only when authorized by the current verification scope;
- run the relevant checks and recorded exact results;
- run authorized gates; if test execution is prohibited, omit the bundled test-running done gate and record NOT RUN plus permitted static checks;
- inspected its own diff;
- documented simplifications and limitations;
- listed remaining manual checks.

Any failed required command keeps the task `IN_PROGRESS` or `BLOCKED`.

## Milestone gate

A milestone passes only when its criteria are checked, tests/lint/build pass, architecture boundaries hold, educational simplifications are documented, and a fresh reviewer reports zero P0/P1 findings.

## Product gate

The learner must be able to open `/` and immediately see the live process flow with Run/Pause and Reset workflow controls, switch seven bottom chart tabs, and inspect bounded node details on hover or keyboard focus; produce a record and follow its bounded token over the actual producer/topic/partition/consumer/commit edges; open `/guide` for the send/read/save walkthrough plus collapsed interview layers; and open `/practice` for the seven category hub and its seven 50-question quiz URLs. Direct `/customize` returns 404. The opening canvas must not contain the retired send/reference buttons, execution-strip metrics, message-flow/readiness/browser-simulation/stage-label noise copy, or a per-message node. The learner must also be able to inspect partition-local offsets, consume records, see assignment and commits, create lag, scale consumers, observe rebalance, run duplicate/loss scenarios, compare independent groups, inspect charts, reload to restore simulator/chart and quiz state, and reset those state domains independently. The visual token layer must remain bounded, reduced-motion compatible, and excluded from persisted state.

Required commands:

```bash
npm run lint
npm run test
npm run build
npm run start
```

If configured, also run `npm run test:e2e`.

## Handoff gate

The root/orchestrator inspects the diff and reruns relevant checks after every implementation handoff. A fresh reviewer checks correctness, architecture, performance, education, and scope. P0/P1 findings return to the developer loop. `COMPLETE` additionally requires the manual browser checklist and local production run to pass.

## Compact chart slice gate

This slice remains `IN_PROGRESS` until all of the following are true:

- `/` shows four compact partition cards with order count, record count, unread/backlog, selected-group success/failure, and C/P/LEO. `unread = max(LEO - P, 0)`, `backlog = max(LEO - P, 0)`, C is committed next offset, P is current read position, and LEO is log-end offset.
- The default graph has no bulky detailed event table/chart; bounded chart tooltips show node-specific metrics and explicit unavailable values where needed.
- The paused reference workload has real producers `checkout-api`, `payment-service`, and `shipping-service`, four partitions, four assigned consumers, eight order keys, 24 records, exactly two committed records, and initial selected-group lag 22. The current failure, retry, commit, and record-retention semantics remain intact.
- Newly produced messages/events, source nodes, per-producer counters, and transit tokens use optional local teaching `producerId` provenance. This is not an intrinsic Kafka record or wire field; older persisted records remain unknown without migration or reset. Automatic ticks round-robin existing producers; no implicit producer is fabricated. The opening toolbar creates fresh unique order keys while the Guide keeps its deterministic `order-7F3K9` sample. Producer and consumer state persists through the existing versioned local storage contract.
- The existing Guide exercise, Practice routes/progress, footer, port `3000` scripts, reset/reference-load behavior, saved layout/viewport behavior, keyboard access, reduced motion, and bounded token rules remain intact.
- Typecheck, lint, tests, production build, browser review, and fresh review pass with zero unresolved P0/P1 findings. Only then may the project return to `READY_FOR_MANUAL_TEST`; user manual acceptance remains required before `COMPLETE`.
