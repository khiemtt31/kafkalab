# Kafka Knowledge Audit

Current UI slice note: [THEME_AND_DIALOG_REMOVAL.md](THEME_AND_DIALOG_REMOVAL.md) removes StepInspector/INSPECT STEP and adds independent light/dark appearance. Normal Activity details and all Kafka teaching/data boundaries remain. The packet/learning gates below are historical for this new slice; no tests are added/run, full automatic test gate is NOT RUN, and theme/dialog browser acceptance is manual section 15.

Status: `CONTENT_REVIEWED_2026-10-04`

Updated: 2026-10-04

This audit is the source boundary for the opening technical chart, Guide, Advanced concept lab, inspectors, scenarios, and Practice question banks. It keeps the app accurate about Kafka concepts while making the browser simulation explicit. The app teaches learner-facing behavior; it does not implement Kafka's broker or wire protocols.

## Authority

Use the direct [Apache Kafka 4.3 documentation](https://kafka.apache.org/43/) as the primary source. The relevant pages are [design, consumer concepts, semantics, and transactions](https://kafka.apache.org/43/design/design/), [topics and partitions](https://kafka.apache.org/43/getting-started/introduction/), [producer configuration](https://kafka.apache.org/43/configuration/producer-configs/), [consumer configuration](https://kafka.apache.org/43/configuration/consumer-configs/), [topic configuration](https://kafka.apache.org/43/configuration/topic-configs/), [broker configuration](https://kafka.apache.org/43/configuration/broker-configs/), [Kafka Streams](https://kafka.apache.org/43/streams/introduction/), [Kafka Connect](https://kafka.apache.org/43/kafka-connect/), and [security](https://kafka.apache.org/43/security/).

The content review used the direct Kafka 4.3 pages on 2026-10-04. Kafka 4.3 supports both the classic and newer `group.protocol=consumer` group protocols; the consumer configuration page says heartbeat and session settings are client-controlled for `classic` and broker-controlled for `consumer`. Copy below states that boundary wherever liveness or assignment details matter. The app must not treat a simulator label as a Kafka guarantee.

## Verified claims and required wording

| Topic | Accurate claim | Required boundary in this app |
| --- | --- | --- |
| Record | A Kafka record can have a key, value, timestamp, and headers. | The browser model stores strings and a timestamp; it does not model serialized bytes or every record field. |
| Topic and partition | A topic is split into partitions. A partition is an ordered log with offsets local to that partition. | State clearly that a multi-partition topic has no single global order. |
| Retention | Reading a record does not delete it. Retention and compaction policies determine when data can be removed or rewritten. | A read, process, failed attempt, or commit must never be described as deleting the record from the simulated topic. |
| Producer | A producer writes records and the partitioner chooses a partition from the record and configuration. | The keyed hash and unkeyed round-robin strategy are educational simplifications. |
| Consumer | A consumer fetches records and application code processes them. | `read`, application success, application failure, and offset commit are separate states. A browser tick is not a broker fetch. |
| Consumer group | Members in one group share partition assignments; distinct groups track progress independently. | The simulator uses one active owner per partition and a deterministic assignment algorithm; it does not claim to implement group coordination. |
| Position and commit | A consumer position is distinct from a group committed offset. The committed offset represents the next position the group should read. | A commit records group progress. It does not prove that an arbitrary business side effect succeeded and does not acknowledge every application layer. |
| Lag | A useful educational view is log-end offset minus the selected group's committed offset. | Label this as the lab's displayed backlog calculation and show the partition and group used. |
| Producer acknowledgements | `acks=all` concerns the partition's required in-sync replicas, together with `min.insync.replicas`; it does not mean every broker in the cluster or every consumer replied. | The reference cluster exposes current ISR, replication factor, min ISR, and required acks. A produce failure is a broker-write acceptance result, not application processing failure. |
| Baseline ISR and min ISR scope | In the baseline Kafka 4.3 path, `acks=all` waits for every current ISR member and `min.insync.replicas` is the minimum ISR admission condition. Kafka 4.3 documents a different `min.insync.replicas` interpretation when Eligible Leader Replicas (ELR) is enabled. | This app teaches the baseline current-ISR rule and explicitly excludes ELR, high-watermark visibility, and broker quorum behavior from its local producer admission model. |
| Replication | Replicas are broker copies of a partition. ISR is the current in-sync subset. | Replica state is educational metadata. It is not a browser copy of broker files. |
| Delivery semantics | Processing before commit can be replayed after a crash; committing before successful processing can skip unfinished application work. | Call this a simulated delivery outcome. Never call the topic record removed or call an offset commit a business success guarantee. |
| Failed-work commit gap | A client can explicitly commit a position beyond failed or unfinished application work; Kafka stores the group position and does not inspect the business result. | This lab applies a named safety policy: both commit modes stop at the latest failed attempt and emit `offset-commit-blocked`; current-position commit can skip only pending unprocessed work. The topic record remains stored. |
| Transactions and exactly once | Kafka transactions can coordinate Kafka writes and offsets under configured client and broker conditions. | The Advanced lab exposes vocabulary and deterministic state only. It must not promise exactly-once external side effects. |
| Schema, Connect, Streams, security | These are Kafka ecosystem concepts with their own clients and services. | The app describes the concept and its boundary; it does not run Schema Registry, Kafka Connect, Streams runtime, SASL, TLS, or ACL enforcement. |

## Current content inventory

The implementation review covered every learner-facing claim in these files:

- `kafkalab/src/components/GuidePage.tsx` — beginner path, interview disclosures, and situation answers.
- `kafkalab/src/components/AdvancedConceptsPanel.tsx` — broker, replication, acks, transaction, policy, liveness, batching, schema, and Streams copy.
- `kafkalab/src/components/StepInspector.tsx` and `kafkalab/src/components/KafkaProcessCanvas.tsx` — object and edge explanations.
- `kafkalab/src/scenarios/index.ts` and scenario modules — guided observations and delivery explanations.
- `kafkalab/src/data/kafka-quiz/*.ts` and `kafkalab/src/data/kafka-quiz/sources.ts` — all 350 Practice questions and explanations.
- `docs/PROJECT_SPEC.md`, `docs/ACCEPTANCE_CRITERIA.md`, and the manual checklist — product promises.

The reviewed quiz banks qualify high-risk claims, including per-partition ordering, retention, independent group offsets, commit versus application effects, transactional visibility, and the distinction between Kafka Connect and Kafka Streams. Keep those qualifications when changing the chart or default workload.

## Exact reviewed coverage

The implementation review covered the following learner-facing content:

| Surface | Files or symbols | Coverage result |
| --- | --- | --- |
| Practice questions | `src/data/kafka-quiz/easy.ts`, `middle.ts`, `hard.ts`, `extreme-hard.ts`, `most-common.ts`, `trick.ts`, `situation.ts` | 7 categories × 50 = 350 questions; all IDs and question texts are unique; every question has four options, one correct option, a concept, and an explanation. |
| Practice sources | `src/data/kafka-quiz/sources.ts` | Direct Apache Kafka 4.3 links cover design, topics, consumers, producer, consumer, topic, and broker configs, semantics, transactions, Streams, Connect, and security. |
| Beginner teaching | `src/components/GuidePage.tsx` | Journey, exercise, concept disclosures, interview answers, common traps, and situation guidance reviewed and corrected for retention, key mapping, commits, lag, liveness, and external side effects. |
| Scenario teaching | `src/scenarios/index.ts`, `src/components/ScenarioPanel.tsx` | All nine scenarios reviewed, including duplicate processing, commit-too-soon, key expansion, lag, scaling, rebalance, and independent groups. |
| Advanced teaching | `src/components/AdvancedConceptsPanel.tsx` | Broker/ISR/acks, transactions, policy flags, heartbeat, batching, schema, and Streams labels reviewed. Local state is explicitly described as educational state. |
| Inspector teaching | `src/components/InspectorPanel.tsx`, `src/components/StepInspector.tsx` | Container labels and the `concept`, `whyChanged`, `relatedCode`, and `simplification` fields were reviewed. The graph-owned target strings were inspected read-only and remain owned by the graph/state workstream. |

## Corrections made in this review

| Claim | File | Source | Change | Verified |
| --- | --- | --- | --- | --- |
| `acks=all` and ISR | `middle.ts`, `hard.ts`, `extreme-hard.ts`, `trick.ts` | [Producer configs](https://kafka.apache.org/43/configuration/producer-configs/), [topic configs](https://kafka.apache.org/43/configuration/topic-configs/) | Wording now says the producer waits for the current ISR; `min.insync.replicas` is the minimum ISR admission check. It does not mean every broker in the cluster replies. | Question text and explanations reviewed; each bank remains 50 questions. |
| `min.insync.replicas` failure | `situation.ts` | [Producer configs](https://kafka.apache.org/43/configuration/producer-configs/), [topic configs](https://kafka.apache.org/43/configuration/topic-configs/) | The scenario now requires the ISR to fall below two. One broker being down can still leave two ISR members and allow the write. | Scenario question is scoped to current ISR size. |
| Producer idempotence | `middle.ts`, `hard.ts` | [Producer configs](https://kafka.apache.org/43/configuration/producer-configs/), [Semantics](https://kafka.apache.org/43/design/design/) | Explanations now limit idempotence to recognized producer retry duplicates and state that it does not deduplicate consumer processing or external effects. | Explanations reviewed against producer sequence/idempotence scope. |
| Consumer poll and liveness | `middle.ts`, `hard.ts`, `GuidePage.tsx` | [Consumer configs](https://kafka.apache.org/43/configuration/consumer-configs/), [Consumer group protocol](https://kafka.apache.org/43/configuration/consumer-configs/) | Copy keeps `max.poll.interval.ms` separate from heartbeat/session behavior and names the classic versus newer consumer protocol difference. | Poll, heartbeat, and commit wording reviewed. |
| Unclean election | `middle.ts` | [Topic configs](https://kafka.apache.org/43/configuration/topic-configs/) | “Committed data” was narrowed to acknowledged data that may be absent from an out-of-sync elected replica. | Explanation now describes availability versus data-loss tradeoff. |
| Read-committed LSO | `extreme-hard.ts` | [Consumer configs](https://kafka.apache.org/43/configuration/consumer-configs/) | LSO explanation now identifies the open-transaction boundary or high watermark when no transaction is open. | Transaction question reviewed for visibility scope. |
| Stream time | `hard.ts` | [Kafka Streams](https://kafka.apache.org/43/streams/introduction/) | Explanation now describes stream time as advancing from record timestamps processed by a task, without claiming a generic watermark implementation. | Streams wording reviewed. |
| Key mapping and partition expansion | `GuidePage.tsx`, `ScenarioPanel.tsx`, `src/scenarios/index.ts` | [Topics and partitions](https://kafka.apache.org/43/getting-started/introduction/), [Producer configs](https://kafka.apache.org/43/configuration/producer-configs/) | Copy now scopes same-key affinity to an unchanged partition count and partitioner, and explains that expansion can create a new ordering boundary. | Scenario and guide copy match the simulator's fixed hash simplification. |
| Retention and lag | `GuidePage.tsx`, `ScenarioPanel.tsx`, `src/scenarios/index.ts` | [Topic configs](https://kafka.apache.org/43/configuration/topic-configs/), [Consumer configs](https://kafka.apache.org/43/configuration/consumer-configs/) | Copy now says retention can move the earliest available offset while a group is behind; reading or committing is not described as deletion. | Slow-consumer and lag explanations reviewed. |
| Transactions and external effects | `extreme-hard.ts`, `AdvancedConceptsPanel.tsx` | [Transactions](https://kafka.apache.org/43/design/design/), [Semantics](https://kafka.apache.org/43/design/design/) | External email/payment effects are explicitly outside Kafka's transaction boundary unless separately integrated. The Advanced panel labels its transaction controls as local teaching state. | Transaction and exactly-once claims reviewed. |
| Scenario rebalance | `ScenarioPanel.tsx`, `src/scenarios/index.ts` | [Consumer group protocol](https://kafka.apache.org/43/configuration/consumer-configs/) | Copy now identifies synchronous round-robin as a simulator simplification and distinguishes eager/cooperative movement in Kafka. | Scenario copy reviewed. |

## Official source notes

The following claims were checked against the current Apache documentation page and linked anchors:

- A topic is partitioned; offsets and ordering are local to each partition, while a multi-partition topic has no single total order. Records are retained according to topic policy rather than removed merely because a consumer read them. See [introduction and topics](https://kafka.apache.org/43/getting-started/introduction/).
- `acks=0`, `acks=1`, and `acks=all` describe producer acknowledgment behavior. In the baseline Kafka 4.3 path, `acks=all` uses the current in-sync replica set, while `min.insync.replicas` can reject a write when the ISR is too small. The Kafka 4.3 topic page explicitly says ELR changes the semantics of `min.insync.replicas`; this app excludes ELR from the baseline lesson. See [producer configs](https://kafka.apache.org/43/configuration/producer-configs/) and [topic configs](https://kafka.apache.org/43/configuration/topic-configs/).
- The classic consumer protocol uses client heartbeat/session settings; `max.poll.interval.ms` limits how long application processing can go without another poll. The newer consumer protocol moves several group timing and assignment controls to the broker. See [consumer configs](https://kafka.apache.org/43/configuration/consumer-configs/).
- `read_committed` excludes aborted transactional records and is bounded by the last stable offset; this does not make arbitrary external side effects transactional. See [consumer configs](https://kafka.apache.org/43/configuration/consumer-configs/) and [transactions](https://kafka.apache.org/43/design/design/).
- Compaction is asynchronous and tombstones are deletion markers for keyed records; compaction and time retention remain separate policies. See [topic configs](https://kafka.apache.org/43/configuration/topic-configs/).
- The high watermark and LEO are replication visibility concepts, not aliases for `min.insync.replicas`. This simulator names them as advanced teaching concepts but does not implement broker replication or high-watermark advancement. See [Kafka 4.3 design](https://kafka.apache.org/43/design/design/).

## Kafka 4.3 evidence boundaries

- The 4.3 producer page defines `acks=0`, `acks=1`, and `acks=all`; `acks=all` waits for the full current ISR, and idempotence requires compatible `acks`, retry, and in-flight settings. See [Kafka 4.3 producer configs](https://kafka.apache.org/43/configuration/producer-configs/).
- The 4.3 topic page defines `min.insync.replicas` as the minimum ISR size for an `acks=all` write and says every current ISR member must acknowledge. It also explicitly warns that ELR changes this configuration's semantics. The quiz and simulator teach the baseline current-ISR path and exclude ELR from that lesson. See [Kafka 4.3 topic configs](https://kafka.apache.org/43/configuration/topic-configs/).
- The 4.3 consumer page supports `group.protocol=classic` and `group.protocol=consumer`. Heartbeat and session settings are client-side for classic and broker-controlled for the consumer protocol; `max.poll.interval.ms` remains the application poll cadence boundary. See [Kafka 4.3 consumer configs](https://kafka.apache.org/43/configuration/consumer-configs/).
- The 4.3 consumer page states that `read_committed` returns records only through the LSO while an open transaction can keep records behind the high watermark. The app describes this as vocabulary and does not implement LSO or high-watermark advancement. See [Kafka 4.3 consumer configs](https://kafka.apache.org/43/configuration/consumer-configs/).
- The 4.3 design page describes ISR replication, commit visibility, unclean election tradeoffs, and compaction; these claims are used as scoped teaching references rather than as a claim that the browser runs broker replication. See [Kafka 4.3 design](https://kafka.apache.org/43/design/design/).

## Residual simulator simplifications

These are deliberate educational boundaries and must remain visible when the related state is shown:

- Keyed routing uses a stable FNV-1a teaching hash. It does not implement every Kafka client partitioner, sticky unkeyed strategy, serializer, or metadata refresh path.
- The browser has one in-memory topic and deterministic local assignments. It does not run a broker, controller, group coordinator, wire protocol, or eager/cooperative rebalance protocol.
- The cluster panel displays leader, replica, and ISR metadata. Broker failure/recovery is a local state transition and does not copy logs, catch up replicas, elect through a real quorum, or run log truncation.
- Transaction controls store open/committed/aborted teaching state. They do not implement the transaction coordinator, producer epochs/fencing, control batches, `read_committed` filtering, LSO advancement, or atomic output-plus-offset commits.
- Heartbeat controls record local heartbeat state and missed counts. They do not send network heartbeats, remove members after a real session timeout, or execute a protocol-specific rebalance.
- Retention, compaction, tombstone expiry, ISR catch-up, high watermark, and LEO are teaching concepts; the local topic does not run broker segment cleanup or replication.
- Schema, Kafka Connect, Kafka Streams, SASL, TLS, ACLs, and external databases or APIs are described as ecosystem concepts only. A schema registration or topology button does not run those services.
- The graph and inspector projections are views of local simulation events. A commit checkpoint is a group progress marker; it does not carry a business payload or prove external completion.
- The lab's failed-work guard blocks both normal processed-offset and current-position commits from crossing the latest failed attempt and emits `offset-commit-blocked`. The explicit current-position lesson demonstrates skipping pending unprocessed work. Real Kafka allows a client to commit past failed work; the stricter guard is a named simulator safety policy, not record deletion.

## Validation evidence

- A focused count/uniqueness check on 2026-10-04 reported 50 questions in each category, 350 total, 350 unique IDs, and 350 unique question texts.
- The final packet/learning root gate on 2026-10-05 passed `npm run typecheck`, lint, 14 test files / 167 tests (including six packet lifecycle/endpoint tests), and production build; see [done-gate.log](../.agents/review/artifacts/packet-learning/done-gate.log). Fresh final source re-review confirms zero remaining P0/P1/actionable P2 findings. Final main browser proof is [after-fix.json](../.agents/review/artifacts/packet-learning/after-fix.json): actual motion, all five lesson states, retry/manual checkpoint separation, validated independent progress, Guide mobile layouts, 11 routes, and no browser errors. Its background trial uses an isolated visibility-state override; native handler behavior is covered by hook tests. Historical compact-flow and technical-chart gates had 146 and 134 tests respectively.
- Final broader [compact regression](../.agents/review/artifacts/packet-learning/compact-regression/browser-evidence.json), [supplemental mobile/storage browser](../.agents/review/artifacts/packet-learning/supplemental-browser.json), and root image review pass for source routing, inspector/retry keyboard focus, persisted producer selection, five-route mobile/motion behavior, readable local comparison scrolling, real-quiz reset independence, and invalid/quota-blocked learning recovery. The final [fresh review](../.agents/review/PACKET_LEARNING_REVIEW.md) records scope and limits. Historical compact/technical chart evidence remains available; user acceptance remains open.

## Current claim scope

The 2026-10-05 packet-lifecycle/guided-experiment slice is `READY_FOR_MANUAL_TEST` and is contracted in [LEARNING_EXPERIMENTS.md](LEARNING_EXPERIMENTS.md). Before this change, Guide/reference material and Practice mainly supported introductory reasoning and recall. The five implemented causal experiments now require predictions and actual local state comparisons; their completion still does not demonstrate production Kafka mastery. Their append animation is a teaching projection rather than a central topic-routing broker; their commit animation represents group progress rather than payload transport or proof of processing. Awaiting commit means progress is unsaved and can coexist with successful application processing. Learning progress is independent under `kafkalab:learning:v1`. Final automated/source-review/main-browser gates pass for the finite frame-painted packet repair. User acceptance and real-world operational learning remain outside those gates; real broker/protocol execution remains excluded.

This review certifies the wording boundary for the current local learner experience: partition-local logs, selected-group positions and outcomes, application failure/retry, group checkpoints, baseline ISR/min-ISR acknowledgement admission, retention language, and the documented boundaries around transactions, liveness, Streams, Connect, schema, security, and broker protocols. It does not certify a real Kafka broker, wire behavior, distributed coordination, broker durability, ELR, LSO/high-watermark execution, or arbitrary external side-effect guarantees. The final browser checks verify that the UI presents these claims with the corresponding local/educational labels.

## Engine/model findings sent to the state workstream

These findings were outside the content-edit boundary of the original audit and were sent to root for the state developer:

1. `src/core/kafka/cluster.ts` now models `minInSyncReplicas` independently from replication factor and lets `acks=all` use the current ISR while using min ISR only as the admission threshold. A reason such as “waits for the full configured replication factor” would teach the wrong rule.
2. The cluster model should label broker recovery as educational metadata unless it models replica catch-up. Restoring a broker directly to ISR is not a Kafka replication protocol.
3. The advanced transaction state should remain visibly state-only unless the engine implements transactional visibility and offset/output coupling. A committed status alone must not imply `read_committed` visibility or external exactly-once effects.
4. Heartbeat state currently represents a manual local reset. It should not be described as a real session-timeout eviction or protocol-specific rebalance.
5. Producer policy flags for idempotence, retries, retention, compaction, and exactly-once are educational state unless the engine applies them to records and delivery outcomes.

## Completed content gates and remaining engineering gates

- [x] Source registry uses direct Apache Kafka 4.3 links and official section pages/anchors.
- [x] Guide, scenario, advanced, and quiz copy avoids claiming that `poll()` proves business processing, that reads delete records, or that commits prove external completion.
- [x] High-risk wording was checked for `durable`, exactly-once, at-least-once, guarantee, always, never, delete, and committed scope.
- [x] Advanced controls and inspector data expose educational simplifications; transaction, heartbeat, schema, Streams, and cluster state are not presented as broker protocol execution.
- [x] Delivery outcomes remain scoped to a group and consumer in the content contract; a stored record is not assigned one global application result.
- [x] This document lists changed claims, official sources, coverage, and verification evidence. A passing build remains insufficient to prove Kafka knowledge accuracy.
- [x] The state workstream finished independent `minInSyncReplicas` and current-ISR behavior; the latest full project gate passed.
- [x] The previous concurrent TypeScript errors were reconciled; the latest typecheck and production build passed.
- [x] Prior technical-chart and compact-chart browser/image gates passed. The current packet/learning slice reaches `READY_FOR_MANUAL_TEST` after its final 167-test gate, clean final source re-review, and passing main production-browser suite; user acceptance is still unchecked.

## Review result

The correction table above is the review result. The improvement-qa gate is not complete while an unqualified claim could make a learner believe that a record was deleted after reading, that a commit confirms business success, that `acks=all` means every broker acknowledged, or that the browser implements Kafka wire behavior.
