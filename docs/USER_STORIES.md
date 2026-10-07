# User Stories

## US-01 Produce and partition a message

As a learner, I can enter a key and value, produce a message, and see the selected partition and allocated offset so I understand append-only logs and partition-local offsets.

## US-02 Compare keyed and unkeyed routing

As a learner, I can produce repeated keys and see deterministic routing while the UI explains the simulator's simplified partitioner.

## US-03 Consume and commit records

As a learner, I can start, pause, resume, crash, and commit a consumer while seeing current processing and committed positions separately.

## US-04 Understand consumer groups

As a learner, I can add and remove consumers and see ownership, idle consumers, and the rule that one active consumer owns a partition inside a group.

## US-05 Compare independent groups

As a learner, I can run two groups against one topic and see that their committed offsets and progress remain independent.

## US-06 Create and recover lag

As a learner, I can make the producer faster than the consumer and observe lag calculated from log-end-offset and committed offset.

## US-07 Observe rebalance

As a learner, I can add or remove a consumer and see the simplified join, revoke, assignment, and completion timeline.

## US-08 Explore delivery semantics

As a learner, I can open **Duplicate processing** or **Commit too soon** and see the consequence of processing before committing or committing before processing. The loss lesson shows that a group skips unfinished work after restart while the record remains stored, without misleading claims about Kafka guarantees.

## US-09 Inspect cause and effect

As a learner, I can select a producer, topic, partition, message, consumer, group, or offset and read what changed, why it changed, and which code/concept represents it.

## US-10 Learn through scenarios

As a learner, I can load, run, reset, and understand guided scenarios for production, keys, lag, scaling, rebalance, duplicates, and multiple groups.

## US-11 Use the lab accessibly

As a learner, I can operate controls with a keyboard, read important values without relying on color, and use the interface with reduced motion enabled.

## US-12 Explore brokers and replication

As a learner, I can create a local broker cluster, inspect replicas and ISR, fail a broker, and observe an educational leader election.

## US-13 Explore delivery controls

As a learner, I can inspect acknowledgements, retry/idempotence, retention, compaction, and dead-letter concepts with explicit simplification labels.

## US-14 Explore application processing concepts

As a learner, I can open and finish transactions, inspect exactly-once concepts, record heartbeats, examine timeout/poll settings, and compare cooperative rebalance behavior.

## US-15 Explore producer and schema processing

As a learner, I can configure batching and compression, register schema versions, and create a Kafka Streams topology that explains its source, sink, and operations.

## US-16 Run locally and deploy later

As a learner or developer, I can install dependencies, run the development app, build a production bundle, serve it locally, and deploy the same client-side app later without a required backend.

## US-17 See the Kafka process flow

As a learner, I can see producers, topics, partitions, groups, consumers, and commits as connected nodes so I understand the path of a record.

## US-18 Inspect flow relationships

As a learner, I can select a node or edge and see what it represents, why it changed, and which Kafka concept it maps to.

## US-19 Organize the process canvas

As a learner, I can pan, zoom, move nodes, use fit view and the minimap, and reset the layout without changing simulation semantics.

## US-20 Open the process canvas

As a learner, I can open `/` and immediately see the message process flow, use simple run/pause, sample-send, and reset buttons, and inspect a selected step without reading a long introduction or opening a configuration panel.

## US-21 Customize the simulation

As a learner, I can open `/customize` to add records, change consumers, run scenarios, configure advanced concepts, and inspect metrics and events.

## US-22 Follow the walkthrough

As a learner with no Kafka background, I can open `/guide` and follow a short send/read/save exercise that starts fresh, explains the difference between reading and committing, and links to my own experiments and the live flow.

## US-23 Inspect flow relationships

As a learner, I can select a node or link on `/` and open an explanation of what changed, why it changed, and which Kafka concept it represents.

## US-24 Read the interface comfortably

As a learner, I can use the warm light interface with readable contrast, visible focus rings, and state labels that do not depend on color alone.

## US-25 Read the opening flow at a glance

As a learner, I can open `/` and understand the process from lively nodes, links, and active packets without a message-flow banner, readiness/status filler, browser-simulation label, or numbered stage strip competing with the canvas.

## US-26 Build interview understanding progressively

As a learner, I can finish the existing Guide send/read/save exercise first, then open collapsed Kafka concept explanations, interview questions and answers, and situation based diagnosis prompts when I am ready for more detail.

## US-27 Choose an interview practice category

As a learner, I can open `/practice` and choose Easy, Middle, Hard, Extreme Hard, Most Common, Trick, or Situation from a category hub that shows my saved progress.

## US-28 Complete a focused quiz

As a learner, I can answer 50 multiple-choice questions in any Practice category, revisit a question, see my selected answer and explanation, and track answered progress out of 50.

## US-29 Resume local learning state

As a learner, I can reload or move between routes and keep simulator state, chart history, selected quiz category/question, and quiz answers in this browser without a backend. I can reset simulator state separately from Practice progress and recover from unavailable or invalid browser storage.

## US-30 Follow an individual record

As a learner, I can produce a record and watch that record travel from the producer to the topic and its allocated partition, then watch the same record move to its assigned consumer when polled and to commit when its offset is actually committed.

## US-31 Distinguish record attempts from graph structure

As a learner, I can see duplicate delivery as another attempt for the same record while the graph remains bounded and does not create one node per record. Tokens use the real partition and consumer relationship that caused the event.

## US-32 Read the technical partition logs

As a learner, I can open the app and inspect one ordered record queue per partition with offset, key, value, timestamp, current position, committed position, LEO, lag, ownership, leader, replicas, and ISR so I can connect the lively flow to Kafka's log structure.

## US-33 Separate application outcomes from Kafka progress

As a learner, I can distinguish queued, read, processed successfully, failed, committed, and skipped delivery states for the selected consumer group. A failure shows an attempt and reason while the stored topic record remains available for retry; both commit modes stop before the latest failed attempt under the lab safety policy and show `offset-commit-blocked`, while current-position commit can skip pending unprocessed work. A commit shows group progress without claiming record deletion or business success.

## US-34 Start with a useful reference workload

As a learner, I can open a paused reference workload with four partitions, a three-broker replication view, four consumers, three real producers, eight order keys, and 24 representative queued, read-pending, processed, committed, and failed records. I can reset to the empty Guide state or explicitly load the reference again without losing saved state during hydration.

## US-35 Learn Kafka claims with trustworthy boundaries

As a learner, I can trust that the Guide, Advanced lab, inspectors, scenarios, and Practice explanations distinguish official Kafka behavior from browser simplifications and link their claim boundaries to current Apache Kafka documentation.

## US-36 See app ownership information

As a learner, I can see an accessible black, warm white, gray, and red footer on every route with the app copyright and supplied creator information, or an explicit pending placeholder until those details are provided.

## US-37 Read a compact partition overview

As a learner, I can open `/` and quickly compare each partition's order count, record count, unread/backlog, selected-group success/failure, and C/P/LEO without a bulky event table competing with the process graph.

## US-38 Inspect record details progressively

As a learner, I can select a partition or record to inspect ordered rows, payload, timestamps, attempts, failure reasons, and replica metadata when I need detail; those fields stay out of the default aggregate cards.

## US-39 Follow real producer and consumer routing

As a learner, I can distinguish `checkout-api`, `payment-service`, and `shipping-service` as real producers with independent counts, select a producer in `/customize`, and follow newly produced records over the matching producer edge to a partition. The local `producerId` is presented as teaching provenance rather than an intrinsic Kafka record field; older records may show unknown source. I can see four actual consumers with deterministic assignments and group-scoped outcomes, and the opening toolbar gives each new sample a fresh order key.

## US-40 Explore a richer paused order workload

As a learner, I can start with eight distinct order keys, four partitions, four consumers, and 24 paused records that include committed, successful, read-pending, queued, and failed application states while preserving genuine retry, commit, and record-retention semantics.

## US-41 Predict, observe, and explain Kafka behavior

As a learner, I can open a collapsed Guide experiment, predict its result, explicitly run a small local workflow, compare actual before/after state, and explain keyed order, replay, skipped work, worker limits, or failure/retry/commit. I can inspect the resulting Flow and save my prediction/completion locally while understanding that this records participation rather than production Kafka mastery.

## US-42 Follow finite packets reliably

As a learner, I can see each new packet start when it appears and disappear after reaching its matching destination. Navigation, reload, backgrounding, pause, and reset do not replay frozen old packets or erase the real stored records, outcomes, events, or progress.

## US-43 Use the flow without a step dialog

As a learner, I can use Flow and Customize without the INSPECT STEP dialog opening or obscuring the interface. I retain compact flow state, normal Customize Activity details, configuration, and Guide experiments. This explicitly supersedes older stories requiring that selected-step overlay.

## US-44 Keep my preferred light or dark theme

As a learner, I can use the far-right header toggle to choose a readable light or dark appearance across every route, keep that preference on reload without a theme flash, and change it without losing records, playback, Practice answers, or learning progress.
