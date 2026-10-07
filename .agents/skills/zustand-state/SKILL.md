---
name: zustand-state
description: Add or review Zustand state adapters, selectors, and simulation controls for the Kafka learning playground.
---

Keep Kafka engine state separate from UI state. Adapt pure engine results into the store instead of moving domain logic into actions or components. Prefer narrow selectors and batched updates so high logical throughput does not cause whole-app rerenders. Preserve deterministic reset and scenario loading.
