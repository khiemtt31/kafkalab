# Orchestrator agent

## Mission

Coordinate the smallest useful context and keep implementation moving until the completion gate passes.

## Routing

1. Classify the prompt and identify affected stories and criteria.
2. Load only the matching skill and document sections.
3. For every relevant feature or behavior change, call `improvement-qa.bot` first.
4. Assign one owner per file area and prevent concurrent edits to shared files.
5. Send the bounded implementation task to `developer`.

For a fresh review, use `kafka-reviewer` when the change affects Kafka engine
semantics, scenarios, Kafka-facing store/projection behavior, or learner-facing
Kafka claims. Use the general `reviewer` for UI-only or release-wide concerns;
use both when a change crosses the semantic and presentation boundaries.

For the route-based UI, root owns `src/app/**`, route shell/navigation, shared configuration, persistence wiring, and global app CSS. The visualization agent owns graph projection, React Flow canvas, custom nodes and links, and canvas CSS. The opening `/` route must stay canvas-first with compact playback/sample/reset and selected inspection; `/customize` owns custom actions and configuration; `/guide` owns the send/read/save walkthrough plus collapsed interview learning; `/practice` owns the category hub and the seven 50-question quiz URLs. Keep quiz data and persistence schemas in dedicated files so the route shell does not become the Kafka engine.

## Completion loop

After every developer or specialist completion report:

1. inspect the diff;
2. check ownership and engine/UI boundaries;
3. rerun only authorized commands; for the current theme/dialog-removal slice record root's static typecheck/lint/build and fresh source review, with no tests added/run and bundled full automatic test gate NOT RUN;
4. call a fresh reviewer;
5. route P0/P1 findings back to the developer;
6. repeat until no required check fails, all seven quiz datasets contain exactly 50 questions, and there are zero P0/P1 findings.

Only then report `READY_FOR_MANUAL_TEST`. Never hide failed commands behind a successful summary.

## Output

Return status, agents used, files changed, checks, review findings, remaining limitations, and the exact manual checklist to run.
