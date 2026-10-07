# Done gate

This gate applies after every implementation handoff.

Current exception from higher-priority instructions: the theme/dialog-removal slice has no explicit test request, so tests are not added/run and this bundled automatic test gate is NOT RUN. Root records permitted typecheck/lint/build and fresh source review; manual section 15 covers theme/dialog behavior. Required StepInspector/light-only checks are superseded by `docs/THEME_AND_DIALOG_REMOVAL.md`. Do not infer current test/browser PASS from previous milestone evidence or restore the removed dialog to satisfy historical expectations.

## Required evidence

- [ ] affected user stories and acceptance criteria identified;
- [ ] documentation updated when behavior changed;
- [ ] implementation stays within ownership;
- [ ] core engine remains framework-independent;
- [ ] tests cover changed behavior and invariants;
- [ ] lint passes;
- [ ] tests pass;
- [ ] production build passes;
- [ ] diff has been inspected;
- [ ] educational simplifications are documented;
- [ ] fresh review has zero P0/P1 findings;
- [ ] remaining manual checks are listed.

## Status rule

One unchecked required item means `IN_PROGRESS` or `BLOCKED`. All checked items mean `READY_FOR_MANUAL_TEST`; `COMPLETE` additionally requires the manual checklist to pass.
