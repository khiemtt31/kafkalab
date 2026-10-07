# Test cases

- [ ] First offset is zero.
- [ ] Offsets increase per partition and do not share a global counter.
- [ ] Same key is deterministic for a fixed partition count.
- [ ] Consumer cannot read an unassigned partition.
- [ ] One partition has at most one active owner per group.
- [ ] More consumers than partitions creates idle consumers.
- [ ] Groups maintain independent committed offsets.
- [ ] Lag equals LEO minus committed offset and is never negative.
- [ ] Consume without commit leaves committed offset unchanged.
- [ ] Rebalance is deterministic on join and leave.
- [ ] Crash before commit can cause duplicate processing.
- [ ] Commit before processing can demonstrate loss.
- [ ] **Commit too soon** restarts with no readable record after committing an unprocessed offset; the stored record remains and the result inspector uses actual state counts.
- [ ] Committing already processed work does not falsely mark it lost.
- [ ] Adding consumers after scenario setup creates unique IDs and preserves group assignments.
- [ ] Produce and consume controls update visible state.
- [ ] Reset returns to the scenario's initial state.
- [ ] Broker failure changes leader/ISR state and recovery restores the broker to ISR.
- [ ] Required acknowledgement settings are retained in the cluster state.
- [ ] Transactions transition open → committed or aborted.
- [ ] Heartbeats record interval, timeout, and missed count.
- [ ] Batching/compression settings are bounded and retained.
- [ ] Schema registration increments versions per subject.
- [ ] Stream topology creation records source, sink, and operations.
- [ ] `npm run build && npm run start` serves the app locally.
- [ ] Flow projection produces stable producer/topic/group IDs and bounded edges.
- [ ] Node selection opens the matching inspector target.
- [ ] Edge selection opens a relationship inspector target.
- [ ] Moving a node does not mutate Kafka simulation state.
- [ ] Reset restores the canonical flow layout.
- [ ] `/` opens with the process-flow canvas as the dominant content and no action forms or metrics dashboard.
- [ ] `/customize` exposes producer, consumer, scenario, advanced, metrics, and timeline actions.
- [ ] `/guide` provides a plain-language walkthrough and links to the other routes.
- [ ] Starting the guide exercise resets the workflow, sends exactly one record, and turns off automatic commit.
- [ ] Read record processes the existing sample without producing more records, then pauses.
- [ ] Save progress advances the committed position, and Start fresh repeats the exercise.
- [ ] Flow playback/sample/reset controls and selected node/link inspection work without exposing configuration forms.
- [ ] Navigating between routes preserves the same simulation state.
- [ ] Warm light theme keeps important text, focus rings, canvas, and chart labels readable.

## Technical opening and knowledge cases

- [ ] Fresh boot loads the deterministic paused compact reference: three real producers, four partitions, four consumers, eight order keys, 24 records, exactly two committed records, and initial lag 22; reset returns the empty Guide state; explicit reference loading is user-triggered.
- [ ] Default `/` shows four aggregate cards with order count, record count, unread/backlog, selected-group success/failure, and C/P/LEO; it has no detailed event table/chart or payload/timestamp/error/replica columns.
- [ ] Selecting a partition opens rows sorted by local offset and shows key/value, timestamp, attempt, selected-group state, current/committed positions, LEO, lag, owner, leader, RF, min ISR, and ISR. `/customize` Activity retains full events.
- [ ] Producer nodes and counts are backed by `checkout-api`, `payment-service`, and `shipping-service`; selection and automatic round-robin preserve optional local `producerId` provenance through newly produced messages, events, and matching transit edges without an implicit `producer-1`. Older records without provenance remain unknown without migration/reset.
- [ ] Opening-toolbar Send order creates a fresh unique order key; Guide Send example remains `order-7F3K9`.
- [ ] Four consumers own one partition each in `analytics-group`, and their assignments/outcomes are actual state.
- [ ] Processing failure records a reason, emits a structured failure event, leaves the topic row and committed offset intact, and retries the same message ID with a new attempt.
- [ ] Processing success remains uncommitted until a separate commit; commit is a group checkpoint and never a deletion or business-success assertion.
- [ ] A failure action leaves the committed cursor unchanged; both commit modes stop before the latest failed attempt and emit `offset-commit-blocked`. Current-position commit can skip only pending unprocessed work in this lab, while the topic record remains stored.
- [ ] Each partition inspector shows a six-row sorted window, pins the oldest unresolved failure, permits scrolling through visible rows, and summarizes hidden rows; superseded attempts are capped at 200 without evicting latest outcomes.
- [ ] Activity pulses expire after `1800ms`; pause/reset/unmount cleanup does not mutate stored records or delivery outcomes, and the execution strip exposes separate `stored`, `checkpoint Σ`, and `commit lag` labels.
- [ ] Keyboard `Enter` on a record row selects only the record inspector, not the parent partition.
- [ ] `acks=all` accepts or rejects using current ISR and min ISR, not full configured replication factor.
- [ ] Guide, Advanced, inspector, scenarios, and all quiz banks pass the source/wording audit in `docs/KAFKA_KNOWLEDGE_AUDIT.md`.
- [ ] The shared footer appears across all routes with copyright and creator details or the explicit pending placeholder.
## Practice and persistence cases

| ID | Case | Expected result |
| --- | --- | --- |
| PRACTICE-01 | Open `/practice` | Exactly seven category links are present with stable labels and progress summaries. |
| PRACTICE-02 | Inspect every quiz data set | Each category has exactly 50 unique questions; every question has four options, one correct answer, a concept tag, and an explanation. |
| PRACTICE-03 | Select, revisit, and change an answer | Selected state, correct state, explanation, current question, and answered/50 progress update deterministically. |
| PRACTICE-04 | Reload a partially answered quiz | Category, selected question, answers, and progress restore; playback is unrelated to quiz hydration. |
| PRACTICE-05 | Reset a category | Only that category's answers and progress clear; simulator/chart state remains unchanged. |
| PERSIST-01 | Change simulator/chart state and reload | Serializable state and chart/timeline history restore, while playback starts paused. |
| PERSIST-02 | Reset simulator after answering quiz questions | Simulator/chart state clears; Practice answers remain. |
| PERSIST-03 | Provide invalid or unavailable storage | App opens with deterministic fallback and recoverable notice. |
| FLOW-01 | Query opening Flow copy | Retired message-flow, readiness, browser-simulation, and stage-label copy is absent; graph names and focusable relationships remain. |
| FLOW-02 | Produce one record | One bounded token carries that record ID from Producer to Topic to its actual partition edge; no record node is created. |
| FLOW-03 | Poll one record | The token uses the event partition and assigned consumer edge for the selected group and includes message identity. |
| FLOW-04 | Commit one record | A token uses the actual consumer → Commit edge only after that record's offset is included in the commit event. |
| FLOW-05 | Duplicate delivery | Same message ID receives a new attempt token; graph node count remains unchanged. |
| FLOW-06 | Overload token queue | At most 24 tokens render; event/timeline state remains correct and token edge routes remain valid. |
| FLOW-07 | Reduced motion/reload | Tokens do not move or use `animateMotion`; active state remains textual, and transient tokens are not replayed after reload. |
