---
name: nextjs-ui
description: Build or review the Next.js, React, and Tailwind interface for the Kafka learning playground.
---

Render domain state from the store; do not implement Kafka algorithms inside components. Keep the UI focused on cause and effect: action, state transition, concept, and explanation. Use semantic controls, keyboard access, textual values for important metrics, responsive layout, and reduced-motion support. Keep visible partition records, timeline entries, chart samples, and animation tokens bounded. Do not claim that retained logical message/delivery history is capped; document reset and short experiments as the current session boundary.

## Process-flow canvas

- Use `@xyflow/react` for the primary node/link canvas.
- Define `nodeTypes` and `edgeTypes` outside component render.
- Use custom nodes and `Handle` ports for Kafka relationships; keep the graph projection separate from `src/core/kafka`.
- Keep node and edge IDs stable, bounded, and derived from the simulator. Never create one node per message.
- Keep node movement as UI state and make reset return to a deterministic layout.
- Configure node/edge focus and accessible names; preserve keyboard selection and reduced-motion behavior.
- Import the React Flow stylesheet globally after the app’s base styles.

## Route shell and novice experience

- Keep `/` almost entirely the polished live process-flow canvas. Do not place forms, dense detail cards, long introductions, or a metrics dashboard on that route.
- Put custom actions and configuration on `/customize`, including producer input, consumer controls, scenarios, advanced concepts, metrics, timeline, and normal Activity detail panels. Keep compact controls on `/`; the explicit current request removes the entire StepInspector/INSPECT STEP overlay from Flow and Customize. Do not replace or restore it from historical docs/tests.
- Put the beginner walkthrough, its send/read/save exercise, and plain-language explanations on `/guide`. Explain that starting the exercise resets the workflow and turns off automatic commits.
- Preserve one client-side simulation state across route navigation.
- Keep canvas labels short. Detailed teaching belongs on `/guide` and normal `/customize` Activity surfaces; graph selection must not open the retired step overlay.
- Provide light default and a charcoal/gray/red dark theme throughout the app, with a far-right accessible shared-header toggle, validated independent persistence, and saved preference applied before visible paint. Theme changes never mutate simulator/playback/quiz/learning state. Use text labels in addition to color; no extra dependencies.
