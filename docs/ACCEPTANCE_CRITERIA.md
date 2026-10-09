# Acceptance Criteria

## Current slice: Flow and Guide learning redesign

The 2026-10-09 request supersedes the Run/Reset-only toolbar, tooltips-as-primary-teaching, and collapsed-only experiment requirements below. The bounded contract is [FLOW_GUIDE_LEARNING_REDESIGN.md](FLOW_GUIDE_LEARNING_REDESIGN.md); earlier theme/chart/learning check results are historical.

- [ ] Flow provides a useful question, meaningful node/relationship variants, visible causal labels, and enough evidence to understand its main result without hovering each node. Tooltips retain optional detail.
- [ ] Flow defaults to a relevant partition path, permits selecting another actual partition, and exposes the whole system only through an explicit comparison action. Group-wide Delivery and simulator-wide Activity remain available where relevant.
- [ ] Failure/delivery content pinpoints actual failed or skipped work, relevant partition/consumer/attempt/reason, and clearly separates reading, application success, and group checkpoint progress.
- [ ] Seven keyboard-accessible chart perspectives project the shared snapshot through stable bounded nodes, independently saved whole-system layouts, and temporary canonical focus layouts. No individual-message graph nodes or invented downstream integrations are introduced.
- [ ] Both Flow and Guide reuse the five existing experiment fixtures, stable IDs, and runner. Prediction/lesson selection alone does not mutate the simulator; explicit Run displays a workflow replacement notice and captures actual evidence.
- [ ] The Guide leads with active lesson choices and a prediction/evidence cycle. The original one-record Send example / Read record / Save progress semantics remain available; repetitive hero/journey/basics copy and generic closing invitations are removed.
- [ ] Retry retains stored record identity and changes the actual attempt/outcome. A separate eligible manual commit advances the checkpoint; stale workflow/group/owner/attempt/read state disables that action.
- [ ] Longer reference concepts and sources remain accessible through concise disclosures. Captured results are labelled separately from current Flow state, and participation is not presented as mastery.
- [ ] Practice, `/customize` removal, INSPECT STEP removal, theme preference, and independent simulator/Practice/learning resets and persistence remain intact.
- [ ] Desktop and 320px/390px views support keyboard, readable light/dark state labels, and reduced motion without relying on animation or page-level horizontal overflow. Local comparison scrolling is labelled and keyboard reachable.
- [ ] Current typecheck, lint, relevant meaningful tests, build, fresh independent review, and browser/manual evidence are recorded. Historical test restrictions do not apply to this new substantial slice. Manual acceptance follows section 17 of [MANUAL_TEST_CHECKLIST.md](MANUAL_TEST_CHECKLIST.md).

## Historical slice: theme toggle and step-dialog removal

These criteria supersede earlier requirements for the selected node/link INSPECT STEP overlay and a light-only interface. Detailed normal Customize Activity panels remain in scope.

- [ ] `StepInspector.tsx` and all imports/render sites are removed from Flow and Customize. No INSPECT STEP dialog, close control, hidden overlay, replacement drawer, or overlay focus trap remains.
- [ ] Flow node/edge/record interactions retain useful selection/focus and do not open the retired dialog; canvas controls, finite packets, compact cards, and stored state remain intact.
- [ ] Customize Activity partition/detail panels, embedded `InspectorPanel`, metrics/timeline, configuration, examples, and Guide exercises remain available without that overlay.
- [ ] A native keyboard-accessible light/dark toggle is the far-right shared-header control on every route, with useful current/action labels and visible focus; it fits at 320px/390px.
- [ ] Missing/invalid preference defaults to light. Validated independent theme storage restores light/dark before first paint without opposite-theme flash or hydration mismatch; blocked storage preserves session switching.
- [ ] Dark styling covers the entire app using charcoal/gray/red and readable text/state/focus, including the canvas, controls/minimap, forms/charts, Guide tables, footer, and Practice feedback. No new dependencies are required.
- [ ] Theme changes and resets do not mutate simulator/chart/playback state, records/offsets/delivery attempts, Practice answers, or learning progress. Other resets preserve theme.
- [ ] Root records permitted typecheck/lint/build and fresh source review. Full automatic test gate is NOT RUN for this slice; prior 167-test/browser evidence is historical, and section 15 manual UI acceptance remains pending.

The bounded contract is [THEME_AND_DIALOG_REMOVAL.md](THEME_AND_DIALOG_REMOVAL.md).

## Core semantics

- [ ] First message in a partition receives offset `0`.
- [ ] Later messages in that partition receive increasing offsets.
- [ ] Partitions maintain independent offsets.
- [ ] Same key and partition count select the same partition.
- [ ] A consumer cannot read an unassigned partition.
- [ ] No partition has two active owners in one group.
- [ ] Every partition has an owner when active consumers exist, unless the group is explicitly in a rebalance transition.
- [ ] Consumers outnumbering partitions produce visible idle consumers.
- [ ] One group is capped at 12 visible consumers so the flow graph remains bounded; the control explains the limit.
- [ ] Groups do not move one another's committed offsets.
- [ ] Consuming without committing does not advance the committed position.
- [ ] Lag equals `logEndOffset - committedOffset` and never becomes negative.
- [ ] Consumer join and leave produce deterministic reassignment and timeline events.
- [ ] Crash-before-commit can read the same record again.
- [ ] **Commit too soon** commits an unprocessed record's current position, restarts, and shows the skipped work while retaining the stored record.
- [ ] Committing an already processed delivery does not emit a false lost-delivery event.

## UI behavior

- [ ] Produce shows key, value, partition, and offset.
- [ ] The opening **Send order** toolbar action creates a fresh unique order key for each new sample; the Guide's dedicated first lesson continues to use `order-7F3K9`.
- [ ] Explicit producer partition selection stays valid after topology changes; submitting cannot target a removed partition.
- [ ] Partitions show bounded aggregate cards with order count, record count, unread/backlog, selected-group success/failure counts, and C/P/LEO; detailed rows remain available through inspection.
- [ ] Consumer controls expose add, remove, pause, resume, crash, speed, commit, and auto-commit simulation.
- [ ] Inspector answers what an object is, its state, why it changed, its Kafka concept, related code, and simplifications.
- [ ] Timeline events are structured and bounded.
- [ ] Charts display real lag and throughput metrics.
- [ ] Scenarios have goal, action, observation, and explanation.
- [ ] The Order lifecycle example appends four events for `order-7F3K9` with the same key, one partition, and increasing partition-local offsets; its explanation distinguishes per-key ordering from cross-partition parallelism.
- [ ] Examples expose these instructions under **How to try it**; the loss example displays actual stored, saved, processed, and restarted-poll counts.
- [ ] Reset restores a deterministic empty learning state.
- [ ] A fresh browser opens a paused deterministic reference workload: `orders`, four partitions, three educational brokers, replication factor 3, `min.insync.replicas=2`, `acks=all`, `analytics-group`, four consumers assigned one partition each, three real producers (`checkout-api`, `payment-service`, `shipping-service`), eight nonsequential order keys, and 24 records with queued, read-pending, processed, committed, and failed delivery states.
- [ ] `Load reference flow` restores that workload only after an explicit user action and does not overwrite persisted simulator state during hydration.
- [ ] The Guide's first sample action uses `order-7F3K9` / `order-created`; `Reset workflow` returns to the empty three-partition state and no UI invents sequential business IDs.

## Architecture and quality

- [ ] Core engine has no UI/framework imports.
- [ ] Educational comments explain major Kafka concepts and simplifications.
- [ ] Visible record windows, event/chart history, and animation activity are bounded; latest group/message outcomes remain available, superseded attempts use a 200-attempt history budget, and the reset boundary is documented honestly.
- [ ] Controls are keyboard accessible and semantically labelled.
- [ ] Meaningful values are not conveyed only by color.
- [ ] Reduced-motion preferences are respected.

## Complete concept tracks

- [ ] Broker cluster creation shows leaders, replicas, ISR, and replication factor.
- [ ] Broker failure shows a deterministic leader election; recovery restores ISR membership.
- [ ] Required acknowledgements (`0`, `1`, `all`) are selectable and explained.
- [ ] Retry/idempotence, retention, compaction, and dead-letter behavior are either runnable in the advanced lab or explicitly marked as educational state with a visible simplification.
- [ ] Transactions expose open, committed, and aborted states; exactly-once language is qualified as a processing concept.
- [ ] Heartbeats expose interval, session timeout, missed heartbeat count, and max poll interval behavior.
- [ ] Cooperative rebalance is represented and distinguished from the synchronous simplified assignment.
- [ ] Batching and compression settings are bounded and visible.
- [ ] Schema versions and compatibility are visible; Streams topology source, sink, and operations are visible.
- [ ] `npm run lint` passes.
- [ ] `npm run test` passes.
- [ ] `npm run build` passes.
- [ ] No backend or real Kafka dependency is required.

## Local delivery

- [ ] A clean `npm install` is sufficient; no environment variables are required.
- [ ] `npm run build` passes.
- [ ] `npm run start` serves the production build at `http://localhost:3000`.
- [ ] The client-side simulation works without a broker, database, or API server and can be deployed later to a compatible Next.js host.

## Process-flow canvas

- [ ] The primary learner view is a React Flow node/link canvas.
- [ ] The opening canvas occupies almost the entire `/` content area and remains the visual focus.
- [ ] Producer, topic, partitions, selected group, and consumer state are represented in the flow.
- [ ] Edges show production and consumption relationships with short labels.
- [ ] Producing, consuming, rebalance, failure, and lag changes update the relevant node or edge state.
- [ ] Selecting a node/edge retains useful graph focus/selection without opening INSPECT STEP; explanations remain on Guide and normal Customize Activity.
- [ ] Nodes can be moved without changing simulation state; reset restores the canonical layout.
- [ ] Pan, zoom, fit view, and minimap work.
- [ ] Nodes and edges are keyboard-focusable with useful accessible names.
- [ ] The graph never creates one node per message and remains bounded.
- [ ] Producing one record creates a visible token for that record ID that travels producer → topic → its allocated partition over the matching edges.
- [ ] Polling one record creates a token on its assigned partition → actual consumer edge; committing creates a token on that consumer → commit edge only for records included in the commit event.
- [ ] Every new produced token carries a stable source event ID, local source `producerId`, record/message ID, partition, offset, phase, and attempt; duplicate delivery reuses the message ID with a new attempt and does not create a graph node. Older persisted records without local provenance remain explicitly unknown.
- [ ] Token routing never uses an unrelated partition or consumer edge. The graph has at most 24 visible tokens; excess activity remains represented by the bounded event/timeline state.
- [ ] Poll events expose message identity and commit events expose committed message IDs or an exact partition/offset range so the visual projection can select the correct records.
- [ ] Token selection does not mutate Kafka state or steal node/edge selection. Record inspection remains available from the record/timeline surface.
- [ ] The visual language uses warm white, black, gray, and red with text labels for state.
- [ ] Reduced-motion mode minimizes flow animation.
- [ ] With reduced motion enabled, no moving token or `animateMotion` runs; matching edge/node state and text still identify the record phase and destination.
- [ ] Transient tokens are not persisted or replayed after reload; persisted simulator, chart, and quiz state remains unchanged.
- [ ] Every fresh packet follows its event presentation clock over the actual SVG edge geometry, completes its matching segment, and disappears within the `1800ms` visual TTL. Later packets cannot inherit the parent document timeline; remount/path updates cannot extend the original event deadline or replay an expired generation.
- [ ] Pause, reset, route changes, hydration/reload, and background/foreground transitions cannot replay stale transits. Fresh sends after returning, including paused sends, still produce a finite correctly routed packet; records, offsets, events, and progress survive decorative cleanup.
- [ ] Append and checkpoint edges are labelled/explained as teaching paths; producer → topic is not presented as a central topic-routing broker and consumer → commit does not imply payload movement or application success.
- [ ] Canvas copy is concise; detailed teaching remains in inspector or scenarios.

## Route experience

- [ ] `/` opens directly into an almost full-screen process-flow visualization.
- [ ] `/` contains no forms, dense detail cards, long introductory guide, or metrics dashboard.
- [ ] `/` has compact run/pause, sample-send, and reset controls without the retired selected-step dialog.
- [ ] `/customize` contains topology, custom producer/consumer actions, scenarios, advanced concepts, metrics, and timeline in clearly named sections.
- [ ] `/guide` contains the beginner walkthrough, plain-language explanations, and a send/read/save exercise.
- [ ] Starting the exercise explicitly resets the workflow, produces exactly one sample, and disables automatic commits.
- [ ] Read record produces no additional messages and pauses after the sample is read; Save progress advances the committed position.
- [ ] Start fresh repeats the exercise deterministically.
- [ ] Navigation between routes preserves the same client-side simulation state.
- [ ] `/guide` links to the relevant action in `/customize` and the live result on `/`.

## Guide interview layers

- [ ] The current send/read/save exercise remains available and keeps its existing behavior: start resets the workflow, produces one sample, disables automatic commit, read pauses after the sample, and save advances the committed position.
- [ ] Interview material is progressively disclosed after the beginner path; all concept, Q&A, and situation sections are collapsed on first visit and use native keyboard accessible disclosure controls.
- [ ] Q&A entries show a concise answer and explanation or common trap without changing simulator state.

## Guided experiments

- [ ] `/guide` adds exactly five initially collapsed experiments: keyed ordering, replay before commit, commit before processing, worker scaling, and failure/retry/manual commit, with stable IDs from [LEARNING_EXPERIMENTS.md](LEARNING_EXPERIMENTS.md).
- [ ] Each requires a prediction before Run and then presents actual captured Before/After evidence, an explanation, an interview follow-up, and its local-simulation boundary. Wrong predictions can be run and revised.
- [ ] A visible Run notice explains that this action replaces the simulator workflow. Opening disclosures, choosing predictions, and clearing learning progress do not mutate simulator/chart state or Practice answers.
- [ ] Results come from existing store/core transitions and actual messages, assignments, offsets, delivery attempts, and events. The evidence is explicitly captured-run evidence; expected values are not fabricated observed readouts.
- [ ] Replay keeps the same message identity with another attempt and no manual commit; commit-too-soon retains one stored record with zero processed deliveries and zero records from restarted poll.
- [ ] Scaling shows five consumers, three partitions, at most three assigned members and two idle members, and one owner of the hot partition. It does not claim production throughput or rebalance protocol execution.
- [ ] Failure retry keeps record identity/count, adds a successful attempt, and leaves manual commit unchanged until the learner explicitly commits. The failed-work guard is named as local lab policy.
- [ ] Validated versioned local progress restores predictions/completion without running experiments or replaying captured results as current live state. Learning, simulator, and Practice resets remain independent; completion does not claim mastery.
- [ ] The one-record Guide exercise stays intact and `/` remains compact. Guide copy says awaiting commit means unsaved group progress, without implying successful business processing is unfinished.
- [ ] At 320px/390px, comparison tables preserve readable columns in a labelled keyboard-focusable local horizontal scroll region, with a visible mobile swipe hint and no page-level horizontal overflow. Touch/keyboard access reaches Before/After/After commit columns.

## Opening flow copy

- [ ] `/` has no visible or accessible `MESSAGE FLOW` strip, `Ready to run`, `Browser simulation`, or separate numbered/stage-label overlay.
- [ ] The removal does not remove useful graph semantics: node and edge names, active packet state, state text, inspector labels, keyboard focus, and reduced-motion behavior remain understandable.

## Practice quiz routes

- [ ] Main navigation includes a `Practice` tab that opens `/practice`.
- [ ] `/practice` links to exactly these seven category URLs: `/practice/easy`, `/practice/middle`, `/practice/hard`, `/practice/extreme-hard`, `/practice/most-common`, `/practice/trick`, and `/practice/situation`.
- [ ] Each category contains exactly 50 unique multiple-choice questions with exactly four options, one correct answer, a stable question ID, a concept tag, and an explanation.
- [ ] Selecting an answer shows the selected answer, correct-answer state, and explanation; progress reports answered count and current question as `answered / 50`.
- [ ] The learner can revisit and change an answer, and can reset a category without changing simulator or chart state.
- [ ] The category hub shows saved progress for each category and links remain usable after reload.

## Local persistence

- [ ] Versioned browser `localStorage` records persist the full serializable simulator state, chart/timeline history, flow selections/layout state, selected Practice category, selected question, and all category answers.
- [ ] Persistence is client-only and does not cause a hydration mismatch. Reload restores the snapshot with playback paused; the learner must press Run to resume simulation.
- [ ] Simulator reset clears simulator/chart state but preserves Practice progress. Category reset clears that category's quiz answers without changing simulator/chart state.
- [ ] Invalid, unavailable, or quota-exceeded storage falls back to deterministic initial state or in-memory operation and exposes a recoverable notice instead of blocking the app.
- [ ] Timers, functions, React objects, Maps, Sets, and other non-serializable runtime values are excluded from storage. Stored records remain versioned and validated before hydration.

## Accessible light and dark themes

- [ ] The default page, canvas, controls, minimap, inputs, and charts use warm light surfaces.
- [ ] Text and focus indicators remain readable in both light and charcoal dark themes; the saved preference is controlled by the far-right shared-header toggle.
- [ ] Important state has text or labels in addition to red/gray styling.
- [ ] All routes work with keyboard navigation and reduced motion.
- [ ] Canvas labels stay short; detailed explanations stay on `/guide` and normal `/customize` Activity without StepInspector.

## Opening compact partition chart

- [ ] The opening canvas includes one compact aggregate card per visible partition without turning the opening route into a forms or metrics dashboard or detailed event table.
- [ ] Each card shows distinct order count, stored record count, unread/backlog, selected-group application success/failure counts, and `C / P / LEO` with accessible full-text labels. `unread = max(LEO - P, 0)`, `backlog = max(LEO - P, 0)`, `C` is committed next offset, `P` is current read position, and `LEO` is log-end offset.
- [ ] Each card keeps owner, leader, replication factor, min ISR, and current ISR available as compact metadata or in its inspector.
- [ ] Selecting a partition opens the bounded ordered row detail with offset, key, value/event label, timestamp, attempt, selected-group state, and failure reason; payload, timestamps, errors, and replica details are absent from the default card.
- [ ] The default graph has no bulky detailed event list/chart. Detailed events remain available in `/customize` Activity and relevant inspectors.
- [ ] Delivery state is scoped to the selected group and attempt: queued, read/in-flight, processed/succeeded, failed, committed, and skipped/lost where a commit-too-soon lesson applies.
- [ ] Processing failure records an application error reason, keeps the topic record stored, leaves the committed offset unchanged, and supports a separate retry attempt for the same message ID.
- [ ] The failure action itself leaves the committed offset unchanged. Both processed and current-position commits stop before the latest failed attempt and emit `offset-commit-blocked`; current-position/Commit too soon can skip only pending unprocessed work. The topic record remains stored. This is the lab safety policy; real Kafka allows explicit commits past failed work.
- [ ] Processing success does not auto-commit. A commit checkpoint is shown as group progress and never as record deletion or proof of arbitrary business success.
- [ ] A read, process result, failure, or commit never removes the record from its partition log. The UI does not promise Kafka wire protocol coverage.
- [ ] Each partition inspector uses a six-row window, pins the oldest unresolved failure when needed, identifies rows outside the window, and keeps the visible rows scrollable for inspection.
- [ ] `offset-commit-blocked` is visible in the timeline/inspector with the failed offset, requested mode, and safety-policy reason.
- [ ] Visual activity pulses clean up after `1800ms` (about two seconds) on normal expiry, pause, reset, unmount, and visibility changes without changing simulation state.
- [ ] The opening execution strip labels `stored`, `checkpoint Σ`, and `commit lag` separately and exposes their selected-group meanings.
- [ ] Keyboard record/detail activation on normal Customize Activity stays accessible; no StepInspector overlay or its focus/Escape behavior remains.
- [ ] The reference seed is paused and visibly useful without claiming live broker activity. It has three real producer identities, four consumers, four partitions, eight order keys, and 24 records. Empty reset and Guide exercise behavior remain unchanged.

## Multiple producer and worker routing

- [ ] Each default producer node is backed by a real producer identity and actual produced count: `checkout-api`, `payment-service`, and `shipping-service`. No decorative duplicate producer nodes are allowed.
- [ ] Producer selection is available in `/customize`; automatic ticks round-robin the existing producer identities and never create an implicit `producer-1` source.
- [ ] Newly produced messages and `message-produced` events carry optional local teaching provenance `producerId`; transit tokens start on the matching producer edge and record inspectors show the source producer. This is not an intrinsic Kafka record or wire-protocol field. Older persisted records without it remain valid and show unknown source without migration or reset.
- [ ] Four default consumers (`consumer-0` through `consumer-3`) are assigned one partition each in `analytics-group`. Consumer counts, assignments, polling, application outcomes, commits, and failures are derived from actual state.
- [ ] Producer identities, per-producer counters, record origins, consumer assignments, delivery attempts, and chart state persist and reload through the existing versioned local storage contract.

## Broker acknowledgement accuracy

- [ ] The educational cluster exposes current ISR and `min.insync.replicas`; `acks=all` accepts a write only when the configured ISR/min-ISR condition is met, not only when every configured replica is up.
- [ ] A produce acceptance/rejection is described as broker-write policy state and is not mixed with application processing success/failure.

## Kafka knowledge and footer

- [ ] Guide, Advanced, inspector, scenario, and all 350 Practice question explanations are audited against [`docs/KAFKA_KNOWLEDGE_AUDIT.md`](KAFKA_KNOWLEDGE_AUDIT.md) and current Apache Kafka documentation links.
- [ ] No learner-facing text claims that reading deletes a record, a commit guarantees business success, `acks=all` means every broker or consumer acknowledged, or the browser implements Kafka's wire/distributed protocols.
- [ ] The shared shell footer appears on every route with black, warm white, gray, and red styling, app copyright, and either the supplied creator details or the explicit `Creator details pending` placeholder. Until supplied, `src/config/site-profile.ts` remains an empty public profile and the UI must not guess an identity.
