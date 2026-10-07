# Test plan

Test the pure engine first, then store adapters, then user interactions. Prefer deterministic fixtures and scenario actions over timing-sensitive assertions.

Required semantic areas: offsets, key partitioning, assignment, idle consumers, independent groups, lag, commits, rebalance, duplicate processing, and message loss.

The explicit loss example is `loss-before-processing` (**Commit too soon**). Test poll-without-process → current-position commit → crash/restart → empty poll, with actual result counts and no removal from the topic. Committing already processed records must not produce a false loss event. Cover generated consumer ID uniqueness after loading/running scenarios.

Required UI areas: produce, consume, commit, lag, add/remove consumer, rebalance timeline, reset, scenario loading, and inspector.

Complete concept tracks also require deterministic tests for broker failure/election, acknowledgements, transaction state, heartbeat state, batching/compression settings, schema versions, and Streams topology creation. The local production command (`npm run build && npm run start`) is a release check.

Flow tests cover deterministic projection, stable IDs, assignment/relationship edges, bounded node counts, node and edge inspector selection, keyboard focus, reset layout, and reduced-motion rendering.

Every test handoff records the exact command and result. Manual visual, accessibility, and responsive checks remain in `docs/MANUAL_TEST_CHECKLIST.md`.

Route coverage is also required: `/` must be canvas-first with compact playback/sample/reset and a selected inspector; `/customize` must expose custom actions and configuration; `/guide` must provide the walkthrough and send/read/save exercise. Test that the exercise resets, produces exactly one record, reads without producing more, pauses after reading, and commits only after the learner saves. Navigate between routes and verify the same client-side simulation is preserved. Verify the warm light palette, keyboard focus, and reduced motion.
## New contract: Guide, Practice, and persistence

- Preserve the Guide send/read/save interaction while testing collapsed interview disclosures as keyboard accessible and simulator neutral.
- Assert the Practice category manifest contains exactly seven slugs and each data set contains exactly 50 unique four-option questions with explanations.
- Test selected answer, correct-answer explanation, answered count, selected question, category progress, answer changes, category reset, and reload persistence.
- Test simulator, chart/timeline, selection/layout, and playback settings persistence across route changes and reload, with playback restored paused.
- Test simulator reset preserves Practice progress and category reset preserves simulator/chart state.
- Test malformed, unavailable, and quota-failing storage recovery without a blocked render.
- Assert `/` does not render the retired `MESSAGE FLOW`, `Ready to run`, `Browser simulation`, or stage-label overlay copy while graph accessibility remains present.

## Record transit contract

- Assert one `message-produced` record creates a producer → topic → exact partition transit with the record/message ID, partition, offset, phase, and source event ID.
- Assert `consumer-polled` includes message identity and creates a token only on the matching assigned partition → consumer edge for the selected group.
- Assert `offset-committed` exposes the committed message IDs or exact partition/offset range and creates consumer → commit transits only for those records.
- Assert duplicate delivery uses the same message ID with a new attempt and never creates a React Flow node. Test the 24-token visible bound while keeping events and state accurate.
- Assert token selection is transient and does not alter node/edge selection, Kafka state, localStorage, or reload behavior. Assert reduced motion has no moving token or `animateMotion` while text and active edge state remain meaningful.

## Compact opening and producer-routing contract

- Assert fresh boot loads the paused four-partition reference workload with producers `checkout-api`, `payment-service`, and `shipping-service`, four assigned consumers, eight order keys, 24 records, exactly two committed records, and initial selected-group lag 22.
- Assert each default producer has real persisted identity and independent count, newly produced messages/events carry optional local teaching `producerId` provenance, `/customize` producer selection works, and automatic ticks round-robin existing producers without creating `producer-1`. Assert older persisted records without the field remain valid and show unknown source without migration/reset.
- Assert `/` renders four compact aggregate partition cards with order count, record count, unread/backlog, selected-group success/failure, and C/P/LEO. The default graph has no detailed event table/chart or payload/timestamp/error/replica columns.
- Assert partition/record inspection exposes the bounded six-row detail, payload, timestamp, attempt, failure reason, and replica metadata while `/customize` Activity retains the full event stream.
- Assert four actual consumers own P0–P3 in `analytics-group`; counts, assignments, polling, outcomes, commits, and failures derive from state rather than decorative nodes.
- Assert existing saved producer identities, counters, layout/viewport, Guide/Practice/footer/storage behavior, port `3000` scripts, and reset/reference-load boundaries remain intact.
- Assert the opening toolbar's Send order creates a fresh unique order key while the Guide's Send example remains the deterministic `order-7F3K9` sample.

## Technical opening and knowledge contract

- Assert fresh boot restores the paused reference workload from `docs/PROJECT_SPEC.md`; reset returns the empty learning state; explicit `Load reference flow` does not overwrite saved state during hydration.
- Assert partition detail rows remain sorted by local offset and expose key/value, timestamp, LEO, current/committed positions, lag, owner, leader, RF, min ISR, ISR, attempt, and group-scoped outcome. These details are absent from the default aggregate cards.
- Assert `DeliveryAttempt.applicationStatus` and `failureReason` are group/attempt scoped. Processing failure emits `record-processing-failed`, retains the topic record, leaves the committed offset unchanged, and retry reuses the message ID with a new attempt.
- Assert processing success and offset commit are separate, commit-too-soon remains a skipped application delivery with the stored record visible, and commit transit describes a checkpoint rather than payload deletion.
- Assert the failure action itself leaves the committed offset unchanged; both processed and current-position commits stop before the latest failed attempt and emit `offset-commit-blocked`. Current-position commit skips only pending unprocessed work in this lab, while real Kafka permits explicit commits past failed work.
- Assert each partition projection renders a six-row window, pins the oldest unresolved failure, exposes scroll access to visible rows, and summarizes rows outside the window; latest group/message outcomes survive while superseded attempts use the 200-attempt budget.
- Assert visual pulses expire after `1800ms` and cleanup on pause/reset/unmount without mutating simulation state; assert the execution strip labels `stored`, `checkpoint Σ`, and `commit lag` separately.
- Assert keyboard `Enter` on a record row opens only the record inspector and does not also select the parent partition.
- Assert `acks=all` uses current ISR and min ISR instead of requiring every configured replica. Test the reference RF3/min ISR2 cluster after one broker failure.
- Perform the content audit in `docs/KAFKA_KNOWLEDGE_AUDIT.md` across Guide, Advanced, inspectors, scenarios, and all Practice banks; record source links and changed claims.
- Assert the shared footer appears on every route with copyright, palette, and supplied creator details or the exact pending placeholder.
