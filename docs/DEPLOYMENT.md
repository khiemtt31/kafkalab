# GitHub and Vercel delivery

The GitHub repository is [khiemtt31/kafkalab](https://github.com/khiemtt31/kafkalab). The repository root includes shared documentation and agent tooling; the deployable Next.js application is in `kafkalab/`.

Production: [KafkaLab](https://kafkalab-six.vercel.app) · [Guide](https://kafkalab-six.vercel.app/guide) · [Practice](https://kafkalab-six.vercel.app/practice).

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

For an existing linked project, deploy from the **repository root**, with Vercel's project Root Directory already set to `kafkalab`:

```bash
npx --yes vercel@latest project inspect kafkalab
npx --yes vercel@latest deploy --prod --yes
```

For a first-time connection, authenticate and link before deploying:

```bash
npx --yes vercel@latest login
npx --yes vercel@latest link --project kafkalab
npx --yes vercel@latest git connect https://github.com/khiemtt31/kafkalab
```

Choose the intended account/team during linking. Local `.vercel/` metadata is ignored by Git. Never commit credentials or paste tokens into chat. If Git connection asks for authorization, install/authorize Vercel's GitHub app for this repository and retry; an authenticated Vercel CLI alone does not authorize GitHub repository access.

A CLI deployment uploads the current local source, including uncommitted application changes. It does not commit or push the repository. The root `.vercelignore` excludes credentials, local build/dependency directories, and development artifacts. A later Git-triggered deployment uses the pushed Git tree, so publish the reviewed changes to GitHub separately when that action is authorized.

## Verify a release

1. Record the release source: Git SHA and clean status for a Git release, or the uploaded local source manifest and pending changes for a CLI release. The initial 20-commit count above is historical.
2. For a Git release, confirm GitHub's production SHA and Application checks workflow pass. For a local CLI release, record the local lint, tests, build, and typecheck results; do not claim GitHub CI covers unpushed changes.
3. Confirm the Vercel deployment is Ready, targets production, and owns the production domain. Verify the recorded deployment ID and uploaded source rather than assuming its Git metadata includes local changes.
4. Open the deployment's production domain. Check `/`, `/guide`, `/practice`, and all seven quiz routes; `/customize` must return 404.
5. Run playback and switch all seven chart tabs. Check focused paths, prediction/run/evidence challenges, separate retry/commit, both themes, mobile layout, optional Details, and saved progress. Follow [manual section 17](MANUAL_TEST_CHECKLIST.md#17-flow-and-guide-learning-redesign--current-slice) for acceptance.

Browser state is origin-specific: existing localhost progress does not automatically transfer to the public Vercel domain. Publishing the app does not create a real Kafka backend or remote progress database. Deployment and automated checks do not replace the user's manual acceptance.

## Current release — 2026-10-10

The interactive Flow/Guide update is live at [kafkalab-six.vercel.app](https://kafkalab-six.vercel.app). Vercel confirms deployment `dpl_Fp36AJWHTspFwE9o5uSe5y1JCTSR` is `READY` with target `production`, and the production alias resolves to that deployment. Its immutable deployment URL is [kafkalab-piv18n0dj](https://kafkalab-piv18n0dj-ttkhiem310503-gmailcoms-projects.vercel.app); Vercel may require authentication for that immutable URL, while the production domain is publicly accessible.

This was a CLI upload of the reviewed local tree, including Flow/Guide and README changes that were uncommitted at deployment time. No Git commit or push was performed during deployment. The upload manifest contains 108 regular files plus one empty directory; all file hashes were verified against current local content, with environment files, credentials, and local artifacts excluded. The application manifest fingerprint (SHA-256 over the ordered application path/SHA-1 pairs) is `dd0a20be22cf3658b230f73ff962f03f5323bd40a4702feef3d04f786fbd30b0`.

The existing application passed local lint, typecheck, 206 tests across 19 files, and its final production build before this documentation-only release preparation. Fresh release review reports P0/P1/actionable P2 = 0. Vercel independently completes `npm ci`, the Next.js production build, and TypeScript compilation. Public HTTP verification passes all ten application routes with 200 and the removed `/customize` route with 404. These checks establish deployment and route availability; manual section 17 remains open for rendered layout, interaction, and learning acceptance.

Local machine evidence, excluded from Git: `.agents/review/artifacts/flow-guide/vercel-inputs.json`, `vercel-source-summary.json`, `vercel-deployment.json`, and `vercel-route-smoke.json`. The [readiness report](../.agents/review/VERCEL_RELEASE_REVIEW.md) and [handoff checklist](../CHECKLIST.md) describe the verification scope.

## Initial release dependency check — 2026-10-07

A clean copy installed from the lockfile passes lint, 193 tests across 18 files, production build, and typecheck. `npm audit --omit=dev` reports zero vulnerabilities. The full audit reports five high-severity findings along the development-only `eslint-config-next` → `fast-glob` → `micromatch` → `braces` chain (GHSA-vfj7-8cjw-p6xm). The currently published `braces` release is 3.0.3 and remains affected; npm's proposed automatic fix downgrades `eslint-config-next` to 14.2.35, which is not a compatible release fix for the locked Next.js 16 app. No forced downgrade was applied. Track this dev-tool issue separately and refresh the audit when a compatible patch becomes available.
