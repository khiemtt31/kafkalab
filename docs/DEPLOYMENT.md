# GitHub and Vercel delivery

The GitHub repository is [khiemtt31/kafkalab](https://github.com/khiemtt31/kafkalab). The repository root includes shared documentation and agent tooling; the deployable Next.js application is in `kafkalab/`.

## Initial Git history

The initial publication is organized into exactly 20 feature-based commits: repository conventions, product specification, framework tooling, core Kafka records, consumers/groups, cluster/advanced behavior, scenarios/reference workload, state adapters, shell/theme, chart projections, canvas, live transit, workspace, learning model, Guide, Practice, regression tests, agent tooling, review/handoff documentation, and CI/deployment.

These commits import the existing project in reviewable parts using current author timestamps. They do not pretend to reconstruct past development dates. The complete final tree, not each intermediate import commit, is the verified application. Dependencies, builds, credentials, Vercel links, browser screenshots/logs, and local archives are excluded.

## Vercel project settings

| Setting | Value |
| --- | --- |
| Git repository | `khiemtt31/kafkalab` |
| Production branch | `main` |
| Root Directory | `kafkalab` |
| Framework | Next.js |
| Node.js | 22.x |
| Install Command | `npm ci` |
| Build Command | `npm run build` |
| Output Directory | Framework default; do not override |
| Required environment variables | None |

Root Directory is a Vercel project setting, not a `vercel.json` property. The app's `vercel.json` supplies the framework/install/build defaults. Use the official [root-directory instructions](https://vercel.com/docs/builds/configure-a-build#root-directory) when importing the repository through the dashboard.

Once the Vercel GitHub integration is connected, pushes to `main` deploy production and pull requests receive preview deployments. GitHub Actions separately runs lint, tests, production build, then typecheck with generated Next.js types. A Vercel build can finish before CI; do not treat deployment success as proof of CI passing.

## Command-line deployment

From the **repository root**, with Vercel's project Root Directory already set to `kafkalab`:

```bash
npx --yes vercel@latest login
npx --yes vercel@latest link --project kafkalab
npx --yes vercel@latest git connect https://github.com/khiemtt31/kafkalab
npx --yes vercel@latest deploy --prod
```

Choose the intended account/team during linking. Local `.vercel/` metadata is ignored by Git. Never commit credentials or paste tokens into chat. If Git connection asks for authorization, install/authorize Vercel's GitHub app for this repository and retry; an authenticated Vercel CLI alone does not authorize GitHub repository access.

## Verify a release

1. Confirm `git rev-list --count main` is 20 for this initial publication and `git status --short` is empty.
2. Confirm GitHub's `main` SHA equals `git rev-parse HEAD` and its Application checks workflow passes.
3. Confirm the Vercel production deployment is Ready and built from that same SHA.
4. Open the deployment's production domain. Check `/`, `/guide`, `/practice`, and all seven quiz routes; `/customize` must return 404.
5. Run playback and switch all seven chart tabs. Check moving activity, pause/reset cleanup, both themes, mobile layout, tooltips, and saved progress. Follow [manual section 16](MANUAL_TEST_CHECKLIST.md#16-focused-chart-tabs-and-tooltips--current-slice) for acceptance.

Browser state is origin-specific: existing localhost progress does not automatically transfer to the public Vercel domain. Publishing the app does not create a real Kafka backend or remote progress database. Deployment and automated checks do not replace the user's manual acceptance.

## Initial release dependency check — 2026-10-07

A clean copy installed from the lockfile passes lint, 193 tests across 18 files, production build, and typecheck. `npm audit --omit=dev` reports zero vulnerabilities. The full audit reports five high-severity findings along the development-only `eslint-config-next` → `fast-glob` → `micromatch` → `braces` chain (GHSA-vfj7-8cjw-p6xm). The currently published `braces` release is 3.0.3 and remains affected; npm's proposed automatic fix downgrades `eslint-config-next` to 14.2.35, which is not a compatible release fix for the locked Next.js 16 app. No forced downgrade was applied. Track this dev-tool issue separately and refresh the audit when a compatible patch becomes available.
