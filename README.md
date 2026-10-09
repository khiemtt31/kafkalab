<div align="center">
  <h1>KafkaLab</h1>
  <p><strong>Follow a record. Pinpoint a failure. Learn why it happened.</strong></p>
  <p>A visual Kafka playground with interactive challenges and 350 interview questions.</p>
  <p>
    <a href="https://kafkalab-six.vercel.app">Open Flow</a> ·
    <a href="https://kafkalab-six.vercel.app/guide">Start learning</a> ·
    <a href="https://kafkalab-six.vercel.app/practice">Practice questions</a>
  </p>
</div>

## Learn through cause and effect

1. **Predict** what happens when a worker fails, a checkpoint moves, or more workers join.
2. **Run** the challenge and follow the actual record, partition, handler, and saved position.
3. **Explain** the changed evidence, try another case, and test your reasoning in Practice.

Explore five cases: same-key ordering, replay after a crash, saving too early, idle workers, and failure → retry → commit. Running a challenge replaces the current workflow with a paused example and keeps your Practice answers.

## Choose your starting point

| Page | What you can do |
| --- | --- |
| [**Flow** · `/`](https://kafkalab-six.vercel.app/) | Follow a focused partition path, see handler outcomes and checkpoints, and pinpoint failed work. Choose a prediction challenge or compare seven perspectives: Flow, Partitions, Consumers, Offsets & lag, Delivery, Replication, and Activity. |
| [**Guide** · `/guide`](https://kafkalab-six.vercel.app/guide) | Work through one active prediction/run/evidence lesson. Send, read, and manually save your first record; open concise reference answers when you need them. |
| [**Practice** · `/practice`](https://kafkalab-six.vercel.app/practice) | Answer questions with explanations and saved progress. Each of the seven categories contains 50 questions. |

Pick a Practice category: [Easy](https://kafkalab-six.vercel.app/practice/easy) · [Middle](https://kafkalab-six.vercel.app/practice/middle) · [Hard](https://kafkalab-six.vercel.app/practice/hard) · [Extreme hard](https://kafkalab-six.vercel.app/practice/extreme-hard) · [Most common](https://kafkalab-six.vercel.app/practice/most-common) · [Trick questions](https://kafkalab-six.vercel.app/practice/trick) · [Situations](https://kafkalab-six.vercel.app/practice/situation).

Essential chart evidence stays visible. Open a node's **Details** button, or focus it and press **F2**, for more context. Show the whole system when you want to compare branches.

## Everything runs in your browser

KafkaLab simulates Kafka in the browser. No Kafka cluster, backend, account, or environment variables are required. Workflow, learning progress, Practice answers, and theme preferences use browser storage. Reload restores a paused workflow; captured experiment results are recreated when you run again.

Saved state belongs to the browser and site origin. The deployed site and `localhost:3000` keep separate progress. Experiments illustrate Kafka behavior; they do not execute real brokers or external business side effects.

## Run locally

Use **Node.js 22.22.2 or newer within Node.js 22**. From the repository root:

```bash
cd kafkalab
npm ci
npm run dev
```

Open [localhost:3000](http://localhost:3000).

To check a change, run these commands sequentially from `kafkalab/`:

```bash
npm run lint
npm run test
npm run build
npm run typecheck
```

Build before typecheck on a fresh checkout so Next.js generates its route types. GitHub Actions runs the same checks.

## Deploy with Vercel

Use the **Next.js** preset, set **Root Directory** to `kafkalab`, install with `npm ci`, and build with `npm run build`. Keep the framework's default output directory. See the [deployment guide](docs/DEPLOYMENT.md) for the full setup.

## Explore the project

[Developer guide](kafkalab/README.md) · [Current Flow & Guide design](docs/FLOW_GUIDE_LEARNING_REDESIGN.md) · [Architecture](docs/PROJECT_SPEC.md)

[Manual acceptance checks](docs/MANUAL_TEST_CHECKLIST.md#17-flow-and-guide-learning-redesign--current-slice) · [Handoff checklist](CHECKLIST.md) · [Current source review](.agents/review/FLOW_GUIDE_LEARNING_REVIEW.md)
