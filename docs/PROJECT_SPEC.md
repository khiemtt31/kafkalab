# Project Specification

## Product goal

Build a complete local-first interactive application that teaches Apache Kafka by simulating behavior and visualizing state changes. It is a learning playground, not a Kafka administration dashboard and not a real Kafka client. The app must run without infrastructure today and remain ready for deployment later.

## Current UI contract — tabbed beginner chart

The opening route is a focused chart workspace with only Run/Pause and Reset workflow controls. Seven native bottom tabs project the same snapshot as Flow, Partitions, Consumers, Offsets & lag, Delivery, Replication, and Activity views. Chart nodes use stable bounded IDs, real snapshot metrics, deterministic layouts, and accessible hover/focus tooltips. The old Customize route, visible send/reference actions, execution strip, and floating inspector surfaces are removed. Guide experiments and internal deterministic fixtures may still use store actions; Guide, Practice, simulator state, learning progress, and HANZO footer branding remain available.

## Architecture

```text
Next.js route shell
  ├── /        → tabbed process-flow canvas
  └── /guide   → beginner walkthrough
  └── /practice → quiz category hub and category quizzes
        ↓
Zustand state adapter
        ↓
pure Kafka simulation engine
        ↓
visualizations, inspectors, charts, and timeline
```

The application must remain statically deployable and must not require a backend, database, broker, API server, WebSocket server, Docker, Redis, authentication, server actions, or external persistence.

## Stack

Required: Next.js, React, strict TypeScript, Tailwind CSS, Zustand, `@xyflow/react`, Vitest, and React Testing Library.

Optional only after evidence: Playwright and Web Workers.

Avoid Redux, RxJS, Framer Motion, Three.js, D3, database libraries, Kafka libraries, backend frameworks, and ORM packages unless a documented requirement proves one necessary. `@xyflow/react` is the required process-flow renderer.

## Core boundary

`src/core/kafka/**` is pure TypeScript. It cannot import React, Next.js, Zustand, or DOM APIs. It returns state transitions, results, and structured events. Zustand adapts that state for React.

## Domain model

```ts
type KafkaMessage = {
  id: string
  producerId?: string
  key: string | null
  value: string
  timestamp: number
  partition: number
  offset: number
}

type Partition = {
  id: number
  messages: KafkaMessage[]
  logEndOffset: number
}

type Topic = { name: string; partitions: Partition[] }

type Consumer = {
  id: string
  groupId: string
  assignedPartitions: number[]
  status: "running" | "paused" | "dead"
}

type ConsumerGroup = {
  id: string
  consumers: Consumer[]
  assignments: Record<number, string>
  committedOffsets: Record<number, number>
}

type DeliveryAttempt = {
  id: string
  groupId: string
  consumerId: string
  messageId: string
  partition: number
  offset: number
  attempt: number
  status: "read" | "processed" | "committed" | "duplicate" | "lost" | "failed"
  applicationStatus: "pending" | "succeeded" | "failed"
  failureReason?: string
  readAt: number
  processedAt?: number
  committedAt?: number
}

type BrokerCluster = {
  brokers: Broker[]
  replicationFactor: number
  minInSyncReplicas: number
  requiredAcks: "0" | "1" | "all"
}
```

`producerId` is optional local teaching provenance. It attributes a newly produced browser-simulation record to the selected local producer service so the source can be inspected after the transient produce event is trimmed. It is not an intrinsic Kafka record field, a Kafka wire field, or a protocol guarantee that a broker records the producing application identity. Records loaded from persisted snapshots created before this field existed remain valid with an unknown source; hydration preserves them without migration or reset.

Committed offsets represent the next offset to read. Lag is always:

```text
logEndOffset - committedOffset
```

Offsets start at zero, increase monotonically, and are local to a partition. There is no global topic offset.

## Required behavior

- keyed messages route deterministically; the simplified hash is documented;
- unkeyed messages use a deterministic educational strategy;
- one active consumer in a group owns a partition at a time;
- consumers may own multiple partitions and may be idle;
- consumer groups maintain independent committed offsets;
- current processing position and committed position are both visible;
- adding or removing consumers causes a visible deterministic rebalance;
- crash-before-commit can show duplicate processing;
- commit-before-processing can show message loss;
- events, lag, throughput, and bounded history come from real simulation state;
- important objects have explanations and source concept mappings through chart tooltips and Guide learning surfaces.

### Event identity used by the flow projection

The simulation event stream is the source of truth for record transit. Newly produced `message-produced` events identify the appended message through `entityId`, include local `producerId` provenance, `partition`, and `offset`, and must retain the source producer identity through the store adapter. Polling an older record whose stored provenance is absent must show `unknown local producer`, not invent an identity. `consumer-polled` events must include `messageId`, `groupId`, `consumerId`, `partition`, `offset`, and `attempt`. `record-processed` events identify the message and its partition/offset and mean application success for that delivery. `record-processing-failed` events identify the same fields plus a safe failure reason and leave the stored record and committed group offset unchanged. `offset-commit-blocked` events identify the group, consumer, partition, failed offset, requested mode, and blocking reason when the lab safety policy stops a commit at the latest failed application attempt. `offset-committed` events include `groupId`, `consumerId`, `partition`, `previous`, `committedOffset`, `mode`, and either the committed `messageIds` or enough exact range information to resolve them. UI selectors must use these fields together; they must not infer a record from the latest event, a visible partition count, or a generic active edge.

The delivery examples include **Duplicate processing** and **Commit too soon** (`loss-before-processing`). The latter polls one record without processing, saves the current next offset, crashes/restarts the worker, and verifies that its next poll returns no records. The record remains stored in the topic; the group's unfinished application work is skipped. Committing an already processed delivery must not create a false loss event. A processing failure records an application delivery failure and leaves the topic record and committed group offset unchanged until a separate retry or commit action.

## Route and novice experience

The app has the following routes with clear ownership:

- `/` is the opening experience: an almost full-screen node-and-link process visualization with Run/Pause, Reset workflow, seven chart tabs, and hover/focus tooltips. It contains no forms, dense detail cards, long introduction, or metrics dashboard. Direct `/customize` visits return 404 because that route is removed.
- `/guide` teaches the process in plain language and includes a guided send/read/save exercise. Starting this exercise explicitly resets the workflow and turns off automatic commits; reading does not produce new records. Its experiment actions remain internal to the learning surface and link back to the live chart on `/`.
- `/practice` is the category hub for interview practice. `/practice/easy`, `/practice/middle`, `/practice/hard`, `/practice/extreme-hard`, `/practice/most-common`, `/practice/trick`, and `/practice/situation` are separate quiz URLs backed by the shared quiz renderer. Each category contains exactly 50 multiple-choice questions with four options, one explanation, and saved progress.

The routes share the same client-side Zustand simulation during in-app navigation. They must not create separate Kafka state models. Simulator and chart state, plus Practice progress, persist in versioned browser `localStorage` and hydrate only after the client mounts. A restored simulation is paused until the learner presses Run. A missing, invalid, unavailable, or over-quota store falls back to a deterministic initial state and shows a recoverable notice; it must not prevent the app from opening.

The opening screen defaults to light warm-white surfaces with black/gray/red accents and can switch to whole-app charcoal/gray/red dark styling through the far-right shared-header theme toggle. The validated saved preference applies before visible paint. The canvas uses short labels; node tooltips and `/guide` supply explanations and technical vocabulary. Important state uses text labels in addition to color, and all routes respect keyboard/reduced motion. Detailed values shown in chart tooltips use explicit `unknown` or `unavailable` labels when the snapshot lacks data.

### Default compact reference workload

The fresh browser boot state is a deterministic, paused reference workload so the compact opening graph is useful on first load without pretending that live Kafka activity happened. It contains topic `orders`, four partitions, a three-broker educational cluster with replication factor `3`, `min.insync.replicas=2`, and `acks=all`, group `analytics-group`, and four consumers (`consumer-0` through `consumer-3`) with one partition assigned to each. Playback and automatic production are stopped; the reference records are static local teaching data. The selected group uses manual commit so application processing and group progress remain visible as separate states.

The reference workload contains three real producer identities: `checkout-api`, `payment-service`, and `shipping-service`. It contains eight distinct nonsequential order keys, two per partition, and 24 records total: `order-created`, `order-paid`, and `order-shipped` records for each order. Each producer owns eight seeded records. These are record values in the partition logs, not separate graph stages or decorative process nodes. Each partition contains six records with offsets `0/1` for created, `2/3` for paid, and `4/5` for shipped. The deterministic keys are P0 `order-7F3K9` and `order-N8R4C`, P1 `order-Q2M81` and `order-A4D92`, P2 `order-R8H27` and `order-B6T90`, and P3 `order-V5X14` and `order-C9L36`. The selected group opens with exactly two committed records (P0/0 and P2/0), P0/1 application-successful but uncommitted, P1/0 failed at application attempt 1 with committed offset 0, P3/0 read and pending, and the remaining records queued. The initial selected-group lag is 22. The exact key-to-partition mapping and producer IDs are deterministic fixtures.

The compact card contract for each partition is:

| Card field | Meaning |
| --- | --- |
| Orders | Distinct order keys stored in the partition; the seed has two per partition |
| Records | Stored records in the partition; the seed has six per partition |
| Unread / backlog | Records remaining from the current read position: `max(LEO - P, 0)`. Commit lag measures the remaining distance from the committed next offset: `max(LEO - C, 0)`. A rewind can make a previously delivered record unread again while its latest delivery outcome remains visible. |
| Success / failed | Latest selected-group application outcomes; a commit is not counted as application success by itself |
| C / P / LEO | Committed next offset, current read position, and log-end offset, respectively; each has an accessible full-text label/tooltip |

Owner, leader, replication factor, `min.insync.replicas`, and ISR remain available as compact metadata and chart tooltips. Full record rows, payloads, timestamps, attempts, failure reasons, and replica details are represented by bounded snapshot values or explicit `unavailable` labels. The default graph has no detailed event table/bulky chart or INSPECT STEP overlay.

The deterministic assignment is P0 → `consumer-0`, P1 → `consumer-1`, P2 → `consumer-2`, and P3 → `consumer-3`. The chart tabs expose aggregate values first and context-specific details on hover/focus. It is not a claim that the browser has contacted a broker. `Reset workflow` continues to return to the empty three-partition learning state used by the Guide; the internal reference fixture remains available to Guide experiments and tests but has no visible main-page load action.

The first explicit Guide sample still uses business-shaped key `order-7F3K9` and value `order-created`, and the Guide exercise keeps its one-record behavior. These nonsequential IDs are display fixtures, not generated offsets.

Example cards expose **How to try it** with Goal, Action, Look for, and Why. Their primary button loads and runs the example before opening Flow. **Commit too soon** retains actual stored/saved/processed/restarted-poll evidence in normal Activity or Guide captured results; it does not open the retired step dialog.

### Guide learning layers

The existing send/read/save exercise is the first visible learning layer and its behavior is stable: starting it resets the workflow, produces one sample, disables automatic commit, reading pauses after the sample, and saving advances the committed position. Additional interview material is progressively disclosed below or beside that exercise. All interview sections are collapsed on first visit, use native keyboard accessible disclosure controls, and do not mutate simulator state. The material includes concise Kafka concept explanations, common interview questions with answers, and situation based diagnosis prompts. Longer explanations belong in these disclosures and chart tooltips, not in the opening canvas.

### Guided experiments

Below the stable one-record Guide exercise, five collapsed, keyboard-accessible experiments require a prediction before an explicit Run action, then show actual captured before/after simulator evidence, an explanation, and an interview follow-up. Their scope is keyed ordering, successful processing before an uncommitted replay, commit-before-processing skips, worker scaling with a hot partition, and failure/retry/manual commit. The exact fixture, state evidence, local progress persistence, and teaching boundaries are defined in [LEARNING_EXPERIMENTS.md](LEARNING_EXPERIMENTS.md). Opening a disclosure or selecting a prediction does not mutate state. Each Run has a visible workflow replacement notice and uses existing simulator actions; the failure lesson keeps manual commit as a separate learner action and guards it against a changed group, owner, read position, attempt, or workflow. Comparison tables retain readable columns at a `540px` minimum width inside a labelled keyboard-focusable local scroll container, with a mobile swipe hint and no page-level horizontal overflow. Learning progress persists in independent `kafkalab:learning:v1` storage, separately from simulator and Practice resets, and does not claim mastery or replay an experiment on reload. Links say **Inspect current Flow** because results are captured evidence and the live workflow can subsequently change.

### Practice quiz contract

Practice has seven fixed category IDs and URL slugs: `easy`, `middle`, `hard`, `extreme-hard`, `most-common`, `trick`, and `situation`. Each category must contain exactly 50 unique multiple-choice questions. Every question has exactly four options, one correct option, a stable ID, a concept tag, and an explanation that is shown after an answer is selected. The quiz shows the selected answer, correct answer state, explanation, answered count, current question, and a `answered / 50` progress indicator. Selecting a question or answer is saved immediately. Learners can revisit questions, change an answer, and reset one category without changing simulator state. The hub links to every category and shows each category's saved progress.

### Opening flow copy boundary

The opening `/` canvas stays visual and concise. It must not show the `MESSAGE FLOW` strip, `Ready to run`, `Browser simulation`, a numbered/stage-label overlay, or the retired INSPECT STEP dialog. Process meaning stays in node/edge labels, packets, state text, Guide, and normal Activity details. This applies to visible/accessibility copy while preserving useful control names and graph accessibility.

## Process-flow canvas

The primary learner surface is a live node-and-link process canvas, inspired by workflow editors such as n8n:

```text
Producers → Topic → Partitions → Consumer Group → Consumers → Commit
```

- Graph nodes and edges are projections of simulation state, not a second Kafka model.
- Stable IDs identify each actual producer (`checkout-api`, `payment-service`, `shipping-service`), the topic, partitions, consumers, commit step, and relationships. Producer nodes show counts and activity derived from their actual `Producer` state and source event metadata; they must not be decorative duplicates. A newly produced record's local provenance can be inspected from its source producer; an older record without `producerId` remains explicitly unknown. The selected group is represented through consumer labels, assignments, and inspector details; it does not require a separate node.
- The canvas supports pan, zoom, fit view, minimap, node movement, keyboard focus, and normal graph selection.
- Node/edge selection never opens StepInspector or a replacement overlay. Its accessible labels remain useful; longer explanations remain on Guide and node tooltips. Overlay close/Escape/focus-restoration requirements are retired.
- Advanced overlays can represent brokers, replicas/ISR, transactions, schemas, Streams, and dead-letter concepts.
- The graph stays bounded: chart nodes show aggregate counts and bounded category/event summaries. Records never become one graph node per message. The default canvas has no detailed event table, payload/timestamp/error columns, or step overlay.
- Individual records appear as bounded moving tokens over the real relationship edges. A token is a visual projection of one `KafkaMessage.id`; it is never a React Flow node and never changes simulation state. At most 24 tokens may be visible at once. When more records arrive, the newest 24 active transits are shown while the complete event/history state remains available in the timeline and inspectors.
- Token edge IDs are stable and must match the actual route: `producer-${producerId}-topic`, `topic-partition-${partition}`, `partition-consumer-${partition}-${consumerId}`, and `consumer-commit-${consumerId}`. The compatibility adapter without a producers list retains `producer-topic`. A token must never travel over an edge whose source, partition, or consumer does not match its event.
- A `message-produced` event starts a token on the matching producer → topic edge and then topic → its allocated partition. New records and events carry local `producerId` provenance; older records may carry no source and must not be assigned a fake producer. A default tick round-robins existing producer identities and never invents a `producer-1` source. A `consumer-polled` event starts a token on that partition → the event's consumer. An `offset-committed` event starts a token on that consumer → commit for each newly committed message in the event's partition/range. `record-processed` changes the record's learner state but does not create a new relationship edge. Duplicate deliveries create a new transit attempt for the same message ID; they do not create graph nodes.
- Flow transit data is derived from event IDs, message IDs, optional producer ID provenance, partition, offset, consumer ID, attempt, phase, and source event ID. Newly produced event contracts expose `producerId`; poll events expose `messageId`; commit events expose committed message IDs or an exact committed offset range. Missing or stale routing metadata drops only the decorative token and leaves the real event visible.
- Selecting a node/edge retains normal graph focus/selection without a step dialog. Tokens are transient visual indicators and never become persisted React Flow selections; tooltips close with pointer/focus changes and are not click-pinned.
- Node positions are UI state and reset to the canonical layout without changing simulation semantics.
- Red marks active flow, errors, failed brokers, and selection. Black, white, and gray carry structure and inactive state.
- Long teaching explanations belong in Guide, normal Activity, and scenario surfaces; canvas labels stay short. Active edge state may summarize visible records but never replace the individual token identity.
- Reduced-motion mode removes token movement and `animateMotion`; matching edge/node state, record phase/destination text, timeline, and normal Activity details remain useful. Visual timers are never restored from storage and StepInspector does not return.
- Normal activity pulses and record transit visuals expire after `1800ms` (about two seconds). Each fresh event supplies its own presentation birth/deadline and generation; packet transforms are painted along the actual SVG edge path from that event age rather than the enclosing SVG/document timeline. At the transit deadline, the exact destination is painted for one frame before removal, with a `64ms` bounded fallback when frames stop. A fresh transit starts when its phase becomes active, while a remounted valid transit keeps only its original remaining lifetime. Path updates do not restart the clock. Pause, reset, route unmount, hydration/reload, and background visibility cleanup invalidate old transits so returning never replays frozen stale packets. New paused-send events still animate finitely without starting playback. Cleanup cancels frames/timers and never removes Kafka records, delivery attempts, or timeline facts.
- Producer → topic → partition is the append teaching path, not an exact Kafka client wire route or a central topic-routing broker. Consumer → commit is the group-checkpoint teaching path; it does not send the business payload to an offset node or prove processing completion.
- The opening execution strip uses explicit labels: `stored` for topic records, `checkpoint Σ` for the selected group's summed committed next offsets, and `commit lag` for the selected group's summed LEO minus committed offsets. These labels describe different quantities and must not be collapsed into one success metric.
- Keyboard activation of normal Activity record/detail controls stays accessible and does not open the retired step dialog. Graph focus/selection remains usable without overlay focus/Escape behavior.

## Partition inspection and delivery detail

The opening `/` view shows bounded chart projections and no step dialog. Tooltips retain the relevant partition-local offsets, key/value or event label, attempt, group outcome, optional local producer provenance, and metadata for LEO/current/committed positions, lag, owner, leader, replicas, min ISR, and current ISR.

Delivery state is group and attempt scoped:

- `queued`: the record is stored in the partition and has not been read by the selected group.
- `read`: the selected consumer has polled it and application work is still in flight.
- `processed`: the application simulation returned success for that delivery, but the group offset may still be uncommitted.
- `failed`: the application simulation returned failure for that delivery. The row shows an error reason and attempt number; the topic record remains stored and the group's committed offset does not advance because of this failure. The lab safety policy also stops both processed and current-position commit modes at the latest failed attempt and emits `offset-commit-blocked`; only pending, unprocessed work can be skipped by the explicit current-position lesson.
- `committed`: the record's offset is included in a group commit checkpoint. This says where the group will resume; it is not a claim that Kafka removed the record or observed an arbitrary business side effect.
- `lost`/`skipped`: an explicit commit-too-soon lesson advanced the group past unfinished work. The row keeps the stored record visible and explains that the application delivery was skipped for that group.

Application success or failure belongs on `DeliveryAttempt`, not on `KafkaMessage`, because two consumer groups can process the same stored record differently. A retry creates another attempt for the same message ID and partition/offset. The engine must expose a processing-failure action/event with `groupId`, `consumerId`, `messageId`, partition, offset, attempt, and failure reason. Processing success does not commit automatically; commit is a separate action. The lab's safety guard blocks both commit modes at the latest failed attempt and emits `offset-commit-blocked`; real Kafka allows a client to explicitly commit past failed work, so this is a named simulator policy. The visual commit token represents an offset checkpoint in the consumer-group metadata path and must not imply that the payload was physically sent to an “offset node.”

The opening chart must not create a React Flow node per record, remove records after reads, or claim every Kafka wire protocol. Chart tooltips retain bounded technical summaries and actual offset/attempt identity; those data must not be deleted by view switching. Latest delivery outcomes and stored topic messages remain in local state; superseded attempts use the `maxDeliveries=200` budget without evicting a latest failure/retry outcome. Failed work leaves commit unchanged and both commit modes stop with `offset-commit-blocked`; current-position commit can skip pending work under the named local lab policy. Real Kafka clients can explicitly commit past failed work. The internal reference fixture remains explicit in store/Guide actions and never overwrites saved state merely on hydration.

## Knowledge accuracy boundary

All Kafka teaching claims in the Guide, Advanced concept lab, inspectors, scenarios, and Practice banks are audited against the direct [Apache Kafka 4.3 documentation](https://kafka.apache.org/43/). The claim-to-source map and required wording are maintained in [`docs/KAFKA_KNOWLEDGE_AUDIT.md`](KAFKA_KNOWLEDGE_AUDIT.md). The app must distinguish per-partition ordering from topic-wide ordering, reads from retention removal, application processing from group commits, and partition ISR acknowledgements from consumer or business success. It must qualify transactions, exactly-once processing, schemas, Connect, Streams, security, and liveness as scoped concepts or local simplifications. It must not claim that browser state is broker durable or that the app implements Kafka's network/distributed protocols.

## App footer

The shared shell renders an accessible footer on every route using the product palette: black structure, warm white content, gray secondary text, and red accent rules or links. It includes the app copyright and a creator line. Public creator details are configured in `kafkalab/src/config/site-profile.ts`; the current creator/brand is `HANZO`. The footer is shell chrome and does not add forms or dense cards to the opening canvas.

## Complete concept tracks

The local app includes deterministic browser simulations for the full learning scope:

- cluster brokers, leaders, followers, replication factor, ISR, broker failure, and leader election;
- acknowledgements, retry and idempotence vocabulary, retention, compaction, and dead-letter patterns;
- transactions, exactly-once processing concepts, heartbeats, session timeout, max poll interval, and cooperative rebalance vocabulary;
- batching, compression, schema evolution, and Kafka Streams topology concepts.

These controls must make their state and timeline effects visible. They do not implement the Kafka wire protocol or a production broker. Each simplification must be explained in the UI or inspector.

## Educational comments

Use `@KafkaConcept`, `@Producer`, `@Consumer`, `@Partition`, `@Offset`, `@CommitOffset`, `@ConsumerGroup`, `@Rebalance`, `@Lag`, `@FailureScenario`, and `@Simplification` as comments. They are not decorators. Do not present simplified coordination as exact Kafka internals.

## Required source shape

```text
src/app
src/core/kafka
src/store
src/components
src/scenarios
src/hooks
tests
```

The route shell and global app CSS are owned by the root/orchestrator. The React Flow graph projection, custom nodes and links, canvas interactions, and canvas CSS are owned by the visualization agent. The pure engine remains independent of React Flow.

Use bounded partition rendering, bounded timeline/chart history, representative animation tokens, batched simulation ticks, and narrow Zustand selectors.

## Session size, persistence, and reset

Visible record windows, timeline/chart samples, and animation activity are bounded. Stored topic messages and the latest delivery outcome for each group/message remain in the persisted simulator snapshot and can grow during continuous production; superseded delivery attempts are capped by the `maxDeliveries=200` history budget. Each partition inspector uses a six-row window that pins the oldest unresolved failure and keeps its visible rows scrollable. Total runtime memory and browser quota are finite. Use short experiments, pause production when inspecting, and reset or apply a fresh workflow between longer runs. Reset clears simulator/chart state but does not erase Practice progress. A separate Practice reset clears the selected category's answers. Persistence uses versioned, JSON serializable records; timers, React objects, Maps, Sets, and functions are never written. Hydration restores data but always leaves playback paused. Corrupt or unavailable storage must be recoverable without a blank screen.

## Runtime boundary and deferred integrations

Do not implement a real Kafka connection, Kafka protocol, SASL, SSL, Schema Registry server, broker persistence, network sockets, real distributed coordination, a backend database, cloud infrastructure, Docker, or Kubernetes.

These are deployment or production-integration decisions for a later product boundary. The complete local app must include the educational broker, replication, failure, transaction, batching, schema, and streams concepts that can be represented deterministically in the browser.
