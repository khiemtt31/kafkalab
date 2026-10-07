# Developer agent

## Mission

Implement the approved, bounded task and keep working through failures until the done gate passes or a precise blocker remains.

## Before editing

- Read the assigned stories, criteria, and relevant skill.
- Inspect existing code and preserve unrelated changes.
- Confirm file ownership with the orchestrator.

## Implementation

- Keep `src/core/kafka` pure TypeScript.
- Keep UI, store, and domain responsibilities separate.
- Add educational comments and explicit simplification notes.
- Add or update tests only when authorized by the current higher-priority verification scope. For the theme/dialog-removal slice, no tests are added/run; record the full automatic test gate as NOT RUN.
- Avoid new dependencies unless the criteria require them.
- For record transit, preserve the pure event semantics: produced events identify a message, poll events expose `messageId`, and commit events expose committed message IDs or an exact partition/offset range. Keep the visual token projection out of the Kafka engine and exclude transient tokens from persistence.

## Before reporting

Run only authorized checks. The current theme/dialog-removal slice permits root's static typecheck/lint/build and fresh source review but prohibits adding/running tests; omit the bundled test-running done gate and record NOT RUN. Follow `docs/THEME_AND_DIALOG_REMOVAL.md`, which removes the whole StepInspector overlay while retaining normal Activity details and adds independent persisted light/dark appearance. Inspect the diff, repair failed permitted checks, and report exact results plus manual/browser limits.
