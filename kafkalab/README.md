<div align="center">
  <h1>KafkaLab · Application</h1>
  <p><strong>Interactive Kafka learning, powered by a browser simulator.</strong></p>
  <p>
    <a href="https://kafkalab-six.vercel.app">Open Flow</a> ·
    <a href="https://kafkalab-six.vercel.app/guide">Guide</a> ·
    <a href="https://kafkalab-six.vercel.app/practice">Practice</a>
  </p>
</div>

The Next.js application lives in this directory. Its learning loop is **predict → run → inspect actual evidence → explain → try another case**. The shared simulator powers the chart, five causal challenges, and the first-record exercise.

## Pages

| Route | Experience |
| --- | --- |
| [Flow · `/`](https://kafkalab-six.vercel.app/) | Focused partition paths with visible handler, failure, ownership, and checkpoint evidence; a selectable prediction challenge; seven chart perspectives. |
| [Guide · `/guide`](https://kafkalab-six.vercel.app/guide) | One active prediction/run/evidence lesson, the Send example / Read record / Save progress exercise, and collapsed reference answers. |
| [Practice · `/practice`](https://kafkalab-six.vercel.app/practice) | Seven categories, 50 questions each, with answer explanations and saved progress: 350 questions in total. |

Category pages: [Easy](https://kafkalab-six.vercel.app/practice/easy), [Middle](https://kafkalab-six.vercel.app/practice/middle), [Hard](https://kafkalab-six.vercel.app/practice/hard), [Extreme hard](https://kafkalab-six.vercel.app/practice/extreme-hard), [Most common](https://kafkalab-six.vercel.app/practice/most-common), [Trick questions](https://kafkalab-six.vercel.app/practice/trick), and [Situations](https://kafkalab-six.vercel.app/practice/situation).

The seven perspectives are Flow, Partitions, Consumers, Offsets & lag, Delivery, Replication, and Activity. Essential evidence appears in the chart and diagnosis; optional node details open deliberately with **Details** or **F2** on a focused node. The retired `/customize` route returns 404.

## Development

Use **Node.js 22.22.2 or newer within Node.js 22**. From the repository root:

```bash
cd kafkalab
npm ci
npm run dev
```

Open [localhost:3000](http://localhost:3000). There are no required environment variables or backend services.

Run checks sequentially from this directory:

```bash
npm run lint
npm run test
npm run build
npm run typecheck
```

Build generates Next.js route types needed by typecheck on a fresh checkout. For a local production preview, stop the development server, then run `npm run build` and `npm run start`; both server modes use port 3000.

## Architecture and state

The application uses Next.js, React, strict TypeScript, Zustand, Tailwind CSS, and React Flow. Vitest and React Testing Library cover Kafka invariants and user interactions.

| Location | Responsibility |
| --- | --- |
| [`src/core/kafka/`](src/core/kafka/) | Pure TypeScript simulation: records, partitions, groups, processing, and offsets. |
| [`src/store/kafka-store.ts`](src/store/kafka-store.ts) | Shared actions, snapshot adaptation, playback, and workspace persistence. |
| [`src/learning/`](src/learning/) | Experiment fixtures, captured evidence, learning progress, and live diagnosis. |
| [`src/components/`](src/components/) | Flow projections, canvas, Guide, and Practice interface. |
| [`tests/`](tests/) | Domain and interaction coverage. |

All Kafka behavior runs locally in the browser. A challenge's explicit Run action replaces the shared workflow with a paused fixture. Selecting a lesson or prediction is passive. Retry and commit remain separate, and captured commits are rejected after relevant live state changes.

Browser storage saves workflow/layout, learning progress, Practice answers, and theme preferences. Resetting one domain preserves the others. Reload restores the simulator paused without rerunning a challenge or restoring its captured evidence. Storage belongs to the browser and origin, so production and localhost maintain separate progress.

These are educational Kafka models, with explicit boundaries for distributed protocols and external side effects. Use short experiments and reset between longer runs; continuously produced records can grow browser storage.

## Vercel deployment

| Setting | Value |
| --- | --- |
| Framework preset | Next.js |
| Root Directory | `kafkalab` |
| Install command | `npm ci` |
| Build command | `npm run build` |
| Output directory | Framework default |

The framework and commands are recorded in [`vercel.json`](vercel.json). See [deployment instructions](../docs/DEPLOYMENT.md).

## Working on a change

Start with the [current Flow & Guide contract](../docs/FLOW_GUIDE_LEARNING_REDESIGN.md), [project specification](../docs/PROJECT_SPEC.md), and [repository instructions](AGENTS.md). Preserve Practice progress, independent resets, and the distinction between captured experiment evidence and live chart state.

The [handoff checklist](../CHECKLIST.md) records engineering checks; the [current source review](../.agents/review/FLOW_GUIDE_LEARNING_REVIEW.md) records review scope. Browser interaction, mobile layout, theme appearance, and learning acceptance require the [current manual checks in section 17](../docs/MANUAL_TEST_CHECKLIST.md#17-flow-and-guide-learning-redesign--current-slice).
