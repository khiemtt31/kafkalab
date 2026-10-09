# Flow and Guide learning redesign

Current product request: 2026-10-09. Implementation and fresh verification are required before manual acceptance. Earlier chart/theme/experiment gate results are historical evidence.

## Problem and outcome

The deployed app has useful Practice questions, but the chart and Guide ask learners to read small aggregate cards and discover explanations in tooltips. The complete chart rarely tells a learner what to investigate, why a failure occurred, or which action will demonstrate the consequence. The Guide repeats the same introduction across a hero, a journey, a basics section, and collapsed experiments.

Flow and Guide should teach through a question, a prediction, a meaningful system view, and observed evidence. A learner should be able to identify the relevant partition, consumer, application outcome, and checkpoint without hovering every node. Scenarios should expose the relationships that explain an outcome and offer a concrete next action.

This contract supersedes the previous Run/Reset-only toolbar requirement, tooltips-as-primary-explanation requirement, and requirement that every experiment start collapsed. It does not restore `/customize` or the floating INSPECT STEP dialog. Practice remains intact.

## Bounded implementation brief

- Keep the shared browser simulator and seven accessible chart perspectives. Present each perspective as a useful question about routing, partition order, ownership, checkpoint/lag, delivery outcomes, replication, or events. Whole-system layouts remain independently saved per view. Focused paths use temporary canonical layouts so prior overview dragging cannot hide the lesson, and focus dragging/panning never overwrites saved overview positions.
- Retain the seven tab IDs/labels: Flow, Partitions, Consumers, Offsets & lag, Delivery, Replication, and Activity. The question above the chart changes with the selected perspective; the Flow learning panel initially selects `failure-retry-commit` without running it.
- Extend meaningful graph relationships where the selected view needs them: actual producer identity, key-to-partition routing, the partition log and leader/replica context, consumer-group assignment, application processing outcome/failure branches, and saved group checkpoint. Nodes represent actual snapshot entities or clearly labelled derived teaching summaries, not invented infrastructure or one node per message. Dashed teaching links distinguish derived outcome relationships from event-backed record transit.
- Put the selected question, essential state evidence, and failure diagnosis in the visible layout. A short inline learning panel can show predictions, an explicit scenario action, captured comparisons, and the next relevant action. Tooltips remain optional detail. Do not obscure the graph with a pinned popup, drawer, or inspector dialog.
- Default to a relevant partition path rather than exposing every branch at once. Flow, Partitions, Consumers, Offsets & lag, and Replication respect the selected focus. Let the learner follow another actual partition or explicitly show the whole system when comparing ownership or topology. Worker scaling defaults to the whole system; Delivery remains group-wide, while Activity shows recent simulator-wide events.
- Reuse the five existing deterministic experiment IDs and their simulator runner: `keyed-ordering`, `replay-before-commit`, `commit-before-processing`, `worker-scaling`, and `failure-retry-commit`. Both routes should use those fixtures and evidence instead of creating another Kafka model. Selecting a lesson or prediction does not change the simulator.
- Make Guide start with a compact lesson chooser and active learning interaction. Preserve the original one-record Send example / Read record / Save progress behavior. Remove repetitive introduction, decorative journey copy, redundant basics, and generic closing invitations. Keep longer concepts and source boundaries available through concise reference disclosures.
- Add no infrastructure or dependencies. Required automated verification concerns scenario evidence, accessible interactions, real-state graph projections, and stale commit protection. Root owns integration checks and final acceptance evidence.

Owned implementation surfaces are the Flow/Guide components, shared chart projection and canvas rendering, their styling, and the existing experiment/progress integration if reuse needs an adapter. The requirements workstream owns this document and narrow updates to Project Spec, acceptance criteria, user stories, roadmap, experiment placement, and manual checks. Root owns `CHECKLIST.md` and validation reporting.

## Teaching invariants

1. A topic is a partitioned log. The drawing's append path is educational; real clients select a partition and communicate with its broker leader. A topic or key-router node is not a separate network service.
2. Offsets start at zero independently in each partition. Same-key routing is qualified by unchanged partition count and partitioner. The local teaching hash is not presented as every Kafka client's algorithm.
3. A partition has one active owner within a group. Consumers can own multiple partitions or remain idle. Groups have independent checkpoints; more consumers do not split a hot partition.
4. Reading, successful application processing, and committing are separate events. Current position and committed next offset must retain full, visible labels where central to the question. Commit lag is `max(logEndOffset - committedOffset, 0)` and can include successfully processed work.
5. A commit saves the next group resume position. It does not delete stored records, prove application success, or send a business payload to an offset service.
6. Failure and replay diagnosis names the actual record ID, partition, offset, owner/consumer, attempt, and safe reason where available. A retry changes the attempt/outcome of the same stored record. Never invent a failure, external side effect, or observed counter.
7. A premature commit can skip unfinished application work after restart while retaining the topic record. Successful-but-uncommitted work can replay. The two outcomes must stay visibly distinct.
8. Failed-work commit blocking is labelled as this lab's safety policy, not a Kafka protocol guarantee. An eligible manual commit remains a separate action after successful retry and is disabled when the workflow, group, ownership, attempt, or read position makes captured evidence stale.
9. Replication nodes show actual brokers/leaders/replicas/ISR from the educational snapshot. `acks=all` concerns current ISR and `min.insync.replicas`, not every broker or consumer. Unexecuted production protocols and downstream integrations remain explicitly scoped.
10. Scenario results are captured run evidence. Current chart values remain live. Navigation/reload cannot silently rerun a scenario, and an earlier captured run must not masquerade as the current workflow.

## Scenario evidence and learning payoff

| Question | Explicit experiment | Evidence to expose | Useful learner conclusion |
| --- | --- | --- | --- |
| Where do events with the same key land? | `keyed-ordering` | Three stored records with the same key, actual partition, and offsets 0/1/2 | Ordering is local to a partition; another partition has an independent sequence. |
| Why can completed work replay? | `replay-before-commit` | Same message ID/partition/offset, a later delivery attempt, processed work, committed next offset still 0 | The restart checkpoint can lag behind application success. |
| Why did restart skip unfinished work? | `commit-before-processing` | Stored record remains, successful outcomes 0, checkpoint 1, restarted poll 0, actual skipped outcome | Saving progress before success can skip application work. |
| Why do extra workers stay idle? | `worker-scaling` | Five consumers, three partitions, actual owners, two idle members, one owner of the hot partition | The partition boundary limits parallelism inside one group. |
| What changes when a failed record is retried? | `failure-retry-commit` | Same stored identity, failure reason and attempt 1, later success, unchanged checkpoint; then separate commit evidence | Retrying processing and saving group progress solve different steps. |

The interface should reveal the cause and relevant path, rather than merely color all failures red. Highlight and label the affected relationships while preserving enough context to explain the result. No moving packet is required to understand a paused or reduced-motion view.

## Interaction and persistence

The learner chooses a lesson and prediction before the explicit experiment Run action. An incorrect prediction still permits running and learning. Display the workflow replacement notice next to Run: the fixture replaces the shared simulator with a paused experiment. Reading reference material, selecting chart perspectives, choosing a prediction, and clearing learning progress do not alter records, offsets, attempts, or Practice answers.

Keep bounded learning progress in the independent existing learning store. Completion records participation in a local experiment. Restoring a prediction or completion does not restore captured evidence as live state and does not claim mastery. Simulator reset, Practice reset, learning progress reset, and theme switching preserve the other domains. Store failure recovery remains usable.

Use native controls, understandable button labels, visible focus, text in addition to color, and announcements for newly captured results. At 320px/390px, controls and lesson choices remain reachable without page-level horizontal overflow; comparison tables may scroll in a labelled, keyboard-focusable local region. Pan/zoom and tooltips supplement the visible teaching content.

## Done conditions and verification

- Flow presents a useful question and enough visible information to understand its main path without opening every tooltip. The delivery/failure view pinpoints actual failed work and separates processing from saved progress.
- Guide offers immediately usable learning choices and feedback while retaining the original one-record exercise. Every reused experiment preserves its stable ID, actual evidence, commit guard, and independent progress behavior.
- The seven views still derive from one simulator snapshot with stable bounded node IDs, accessible tabs, independent whole-system positions/viewports, and temporary canonical focus layouts. Practice categories/questions/progress are preserved; `/customize` stays removed; no floating inspector returns.
- Current typecheck, lint, meaningful relevant tests, and production build pass. Build and typecheck run sequentially. Fresh independent review verifies the final implementation against this contract and fixes actionable defects.
- Fresh browser/manual checks cover the main learning cycle, failure diagnosis, retry/commit separation, keyboard access, 320px/390px layout, light/dark, reduced motion, route navigation, and reset/reload independence. Record actual evidence and precise remaining limits; historical test/build/browser evidence cannot certify the redesign. If the available browser capability cannot attach a session, report those visual/manual checks as pending rather than claiming a pass.

Use [MANUAL_TEST_CHECKLIST.md](MANUAL_TEST_CHECKLIST.md), section 17. Engineering completion and `READY_FOR_MANUAL_TEST` do not imply that the user has accepted the redesigned learning experience. Deployment is outside this request.
