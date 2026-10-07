# Kafka Visual Learning Lab

This repository contains the complete local-first Kafka Visual Learning Lab, its implementation workflow, Codex skills, sub-agent roles, completion gates, and manual test plan.

The simulator teaches Kafka by making this loop visible:

```text
user action → simulator transition → Kafka concept → visible state change
```

The opening screen is a focused, almost full-screen node-and-link learning canvas. Seven Excel-style tabs project the same Kafka snapshot as Flow, Partitions, Consumers, Offsets & lag, Delivery, Replication, and Activity views. React Flow provides pan/zoom/fit/minimap, node movement, and accessible selection. Nodes expose contextual details on pointer hover or keyboard focus through accessible tooltips. The main page keeps only Run/Pause and Reset workflow; Guide, Practice, saved records, learning progress, and HANZO branding remain available.

### Application routes

| Route | Purpose |
| --- | --- |
| `/` | Beginner-friendly live chart with seven selectable projections, Run/Pause, Reset workflow, and accessible node tooltips. |
| `/guide` | The original one-record send/read/save lesson, five collapsed prediction/run/observe/explain experiments, and interview reference material. |
| `/practice` | Interview practice category hub with saved answered counts. |
| `/practice/easy` through `/practice/situation` | Seven fixed 50-question quiz categories with selected-answer explanations and progress. |

It is a teaching simulator rather than a Kafka administration dashboard or a real broker client. A fresh browser opens a paused reference workload with three real producers (`checkout-api`, `payment-service`, `shipping-service`), four partitions, four consumers, eight distinct order keys, 24 records, and a three-broker RF3/min-ISR2 educational cluster. Newly produced records may carry optional local `producerId` teaching provenance for source inspection; this is not a Kafka record or wire-protocol field, and older saved records remain valid with unknown source. The internal reference fixture remains available to Guide experiments and deterministic tests, but has no visible main-page load or send action. Reset returns to the empty Guide learning state. The simulation is intentionally self-contained so it runs locally without infrastructure and can be deployed later as a client-side Next.js app. Application processing failure remains a delivery-attempt state; reading or committing never removes the topic record.

The five Guide experiments cover keyed ordering, replay after success without commit, skipping work by committing too soon, worker scaling with a hot partition, and failure/retry/manual commit. Choose a prediction before Run; the visible notice explains that Run replaces the workflow. Actual captured Before/After state explains each result, and the retry lesson requires a separate guarded manual commit. **Inspect current Flow** opens the shared live workflow, which can differ from an earlier captured run. Completion records participation rather than production Kafka mastery; advanced flags and metadata remain educational boundaries.

## Read first

1. [guide.md](guide.md) — how to turn a request into a documented, tested change.
2. [CHECKLIST.md](../CHECKLIST.md) — the complete build and manual-test checklist.
3. [docs/PROJECT_SPEC.md](../docs/PROJECT_SPEC.md) — architecture and Kafka semantics.
4. [docs/DEFINITION_OF_DONE.md](../docs/DEFINITION_OF_DONE.md) — completion gates.
5. [AGENTS.md](AGENTS.md) — routing and ownership rules for Codex.

## Runtime boundary

The application is client-side and deployable later to a compatible Next.js host. The core simulation is pure TypeScript and is adapted into Zustand before React renders it. The route shell owns navigation and delivery configuration; the visualization owns graph projection and canvas styling.

Required stack:

```text
Next.js, React, strict TypeScript, Tailwind CSS,
Zustand, @xyflow/react, Vitest, React Testing Library
```

Playwright and Web Workers are optional tools; the complete product does not depend on either one.

## Run locally

The earlier packet/learning handoff left a production server at `http://localhost:3000`; that historical server/browser record does not verify the current theme/dialog-removal changes. Before a later manual server-mode switch or rebuild, stop the app and verify port 3000 is free:

```bash
npm run stop
lsof -nP -iTCP:3000 -sTCP:LISTEN
```

The port check should show no listener. Then start one chosen mode:

```bash
npm install
npm run dev
```

Open `http://localhost:3000`. Before production verification, repeat the stop/free-port check above, then use:

```bash
npm run lint
npm run typecheck
npm run build
npm run start
```

The app has no required environment variables and no backend dependency. Deploy later to a Node-capable Next.js host with `npm run build` followed by `npm run start`, or adapt the delivery shell to a static host after choosing the hosting target.

The automated checks cover the seven projections, bounded graph identities, replication empty/cluster states, tooltip semantics, tab navigation, persistence migration, Guide behavior, Practice progress, and the deterministic reset contract. Visual coverage, keyboard/mobile layout, tab scrolling, tooltip placement, reduced motion, and direct `/customize` 404 behavior remain manual checks.

Use short experiments and reset between longer runs. The graph, six-row partition windows, timeline/chart samples, and animation activity are bounded. Stored topic messages and the latest delivery outcome for each group/message persist in browser storage; superseded delivery attempts use the 200-attempt history budget. The oldest unresolved failure stays reachable in a partition window, and visible rows can be inspected by scrolling. Learning predictions/completion use independent validated `kafkalab:learning:v1` storage. Simulator reset, Practice category reset, and **Clear learning progress** each preserve the other progress stores. Playback restores paused after reload, without packets or captured learning results being replayed. Continuous production can increase processing cost and browser quota use.

The footer reads public creator details from `src/config/site-profile.ts`. The current creator/brand is `HANZO`, and the footer displays the current-year copyright with that attribution on every route.

## Completion status

The current chart simplification is implemented. Static checks, the automated test suite, and the production build are the local verification gates; browser-sized layout checks remain manual.
