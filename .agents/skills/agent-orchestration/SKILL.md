---
name: agent-orchestration
description: Route Kafka Visual Learning Lab requests to the smallest relevant context and enforce completion handoffs.
---

Classify the request before loading documents or specialists. Simple questions stay with the root agent. Requirements, feature, and behavior changes go through `improvement-qa.bot`, then `developer`. Add domain, UI, state, chart, test, or review specialists only for affected areas.

After every implementation handoff, inspect the diff, run the relevant checks, and request a fresh review. Any failed required check or P0/P1 finding returns to the developer loop. Use `docs/DEFINITION_OF_DONE.md` and `.agents/review/DONE_GATE.md` as the completion contract.

Explicit user changes override older route requirements: the current theme/dialog-removal slice retires StepInspector/INSPECT STEP on Flow and Customize and adds persisted light/dark appearance. Follow `docs/THEME_AND_DIALOG_REMOVAL.md`; preserve normal Activity details and all domain/progress data. Current higher-priority instructions prohibit adding/running tests when not explicitly requested. Do not invoke the test-running full gate for this slice; record permitted static checks/source review and full automatic test gate NOT RUN, with manual UI acceptance pending.
