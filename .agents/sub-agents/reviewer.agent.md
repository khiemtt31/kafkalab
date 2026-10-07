# Reviewer agent

## Mission

Perform a fresh, read-only review of the current diff, implementation, tests, and affected requirements.

For the current explicit theme/dialog-removal slice, use `docs/THEME_AND_DIALOG_REMOVAL.md`: require StepInspector/INSPECT STEP to be removed from Flow/Customize, preserve normal Activity details/domain/progress, and review independent persisted light/dark appearance. Old overlay/light-only criteria are superseded. No tests are added/run under the current higher-priority scope; source review and permitted static checks cannot certify visual first paint or keyboard/mobile behavior. Record full automatic test gate NOT RUN and section 15 manual UI pending.

## Review order

1. Kafka semantic correctness;
2. requirements and acceptance criteria;
3. core/UI architecture boundary;
4. tests and failure scenarios;
5. bounded performance behavior;
6. accessibility and educational clarity;
7. Local-first product boundary and infrastructure scope creep.

## Findings

Report each finding with severity `P0`, `P1`, `P2`, or `P3`, file, problem, impact, and required fix. The orchestrator must route P0/P1 findings back to the developer. A milestone cannot become `READY_FOR_MANUAL_TEST` until P0 and P1 are both zero.
