# KafkaLab

A visual, local-first Kafka learning playground built with Next.js, React, TypeScript, Zustand, and React Flow.

Explore seven live chart views, consumer groups and checkpoints, delivery failures and retries, guided experiments, and seven 50-question interview practice categories. This is an educational browser simulator, not a connection to a real Kafka cluster. Progress and workflow state stay in your browser's local storage; no backend or environment variables are required.

## Run locally

Use Node.js 22.22.2 or newer within the Node.js 22 release line, as required by the locked test dependencies. The application is in the **`kafkalab/` subdirectory**, not the repository root.

```bash
cd kafkalab
npm ci
npm run dev
```

Open http://localhost:3000. Routes include `/`, `/guide`, `/practice`, and seven `/practice/<category>` pages. The retired `/customize` route returns 404.

## Check a change

From `kafkalab/`:

```bash
npm run lint
npm test
npm run build
npm run typecheck
```

Build before typecheck on a fresh checkout so Next.js generates its route and environment types. GitHub Actions runs these checks for pushes and pull requests. Stop the development server before switching the same port to `npm run start`.

## Deploy

Vercel must use the Next.js preset and **Root Directory `kafkalab`**. The install command is `npm ci`, the build command is `npm run build`, and the output directory uses the framework default. See [deployment instructions](docs/DEPLOYMENT.md).

## Project references

- [Application overview](kafkalab/README.md)
- [Architecture and teaching boundaries](docs/PROJECT_SPEC.md)
- [Implementation and manual handoff](CHECKLIST.md)
- [Manual acceptance checklist](docs/MANUAL_TEST_CHECKLIST.md)
- [Latest chart repair review](.agents/review/CHART_LIVE_UI_REVIEW.md)

Historical review reports refer to local browser screenshots/logs under `.agents/review/artifacts/`. Those machine-specific files are intentionally excluded from Git. Repeatable checks remain in `.agents/scripts/`; do not interpret an archived report as verification of a later change.
