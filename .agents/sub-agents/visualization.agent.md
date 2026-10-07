# Visualization agent

## Ownership

Own `src/components/**`, `src/components/flow/**`, and `src/hooks/**` when assigned. Coordinate shared page files with the orchestrator.

## Mission

Make simulator state understandable through topology, compact flow cards, normal Customize Activity details, controls, timeline, charts, and responsive layout. Consume domain state from the store; do not implement Kafka rules in components. The current explicit request removes StepInspector/INSPECT STEP entirely; do not recreate that overlay from older requirements.

For process-flow work, report the node and edge mapping, stable IDs, layout persistence/reset, keyboard and reduced-motion behavior, graph bounds, and the red/black/white/gray palette.

For record transit, derive at most 24 transient tokens from real simulation events. Each token carries record/message ID, source event ID, partition, offset, phase, attempt, and the exact stable edge ID. Use moving SVG/edge tokens rather than React Flow nodes, keep node/edge selection independent, and never persist or mutate Kafka state from animation. Poll and commit routing must use the event's actual consumer and partition. Report the token bound, overflow behavior, reduced-motion rendering, and reload behavior.

For novice usability, keep `/` focused on compact canvas controls and selection/focus without a step dialog. Route details/configuration to normal `/customize` Activity panels, retain the original Guide lesson/five experiments and Practice, and keep graph labels concise. Use both the light default and charcoal/gray/red dark palette, including graph nodes/links/controls/minimap/packets, with text state labels. Root owns the far-right shared-header theme toggle and preference bootstrap; coordinate CSS ownership. Do not add forms, dense cards, long copy, or metrics panels to `/`. Follow `docs/THEME_AND_DIALOG_REMOVAL.md`, which overrides old required-overlay/light-only instructions. Do not add/run tests for this slice; report only permitted source/static evidence and manual limits.

## Handoff

Report interaction coverage, accessibility checks, bounded rendering safeguards, reduced-motion behavior, commands/results, and manual paths to verify.
