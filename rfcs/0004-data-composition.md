# RFC 0004: Principled Data Composition

**Status**: Draft

**Authors**:
- [Jason Gustafson](https://github.com/hachikuji)

## Summary

This RFC explores the opportunity for data composition in OpenData. Traditional databases are isolated silos with rigid ingest APIs—composing them requires RPC boundaries, connector services, and operational overhead at every hop. OpenData's shared storage and library-first architecture suggest a different model might be possible: principled composition with strong guarantees and simpler deployment.

We propose terminology for discussing composition and sketch two modes worth exploring: *internal composition*, where multiple data systems share a single SlateDB instance, and *external composition*, where systems in separate instances integrate through common semantics. We also examine how SlateDB's specific capabilities—arbitrary checkpoints and cheap clones—could enable cross-system snapshot semantics that would be impractical with traditional storage.

The goal is not to define specific APIs, but to map the landscape and stimulate thinking about what composition patterns OpenData's unique architecture could enable.

## Motivation

Modern applications rarely need just one data system. A usage-based billing platform might combine an event log (capturing usage events) with a timeseries database (aggregating metrics over time). A recommendation system might pair a vector database (similarity search) with a log (tracking user interactions).

Traditionally, composing disparate systems is painful. Each system has rigid ingest APIs, its own operational profile, and no understanding of the others. Glue code must bridge semantic mismatches and handle failures across system boundaries.

OpenData is uniquely positioned to do better. All OpenData databases share a common storage substrate (SlateDB), common infrastructure, common data conventions, and a library-first architecture. This shared foundation suggests composition patterns that would be impractical with disparate systems—embedding readers and writers in the same process, sharing storage for transactional guarantees, or leveraging common semantics for type-safe transformations.

This RFC explores what that might look like.

## Goals

- Define terminology for discussing data composition in OpenData
- Sketch the conceptual landscape for internal and external composition
- Identify potential guarantees and tradeoffs of each composition mode
- Stimulate thinking about what OpenData's shared foundations could enable
- Build consensus on whether this direction is worth pursuing

## Non-Goals

- Proposing specific APIs or wire protocols (left for follow-up RFCs)
- Defining particular composition patterns (e.g., log-to-timeseries pipelines)
- Implementation details or timelines
- Changes to existing system architectures

## Design

### Terminology

**Data System**: A logical database with its own data model, query semantics, and storage layout. OpenData currently includes three data systems: Log, TSDB, and Vector.

**Slate**: A SlateDB instance. Each slate is an independent LSM tree with its own manifest, compaction, and caching.

**Composition**: The combination of multiple data systems to serve a unified use case, with defined semantics for how data flows between them.

**Embedding**: A deployment pattern where multiple system components (readers, writers) run in the same process, avoiding RPC boundaries. Enabled by OpenData's library-first architecture.

### Composition Modes

#### Internal Composition

Internal composition would place multiple data systems within a single slate. The systems would share:

- A single SlateDB manifest (atomic updates)
- A single compaction process
- A single write-ahead log

**Potential guarantees:**

- **Transactional writes**: A single write batch could atomically update multiple data systems. Either all updates succeed or none do.
- **Consistent reads**: Snapshot isolation would apply across all systems in the slate. A reader sees a consistent view of all data as of a single point in time.
- **Unified compaction**: Records from different systems would be compacted together, potentially enabling cross-system optimizations.

**Tradeoffs to consider:**

- **Coupled scaling**: All systems in the slate scale together. If one system has significantly different access patterns (e.g., write-heavy log vs. read-heavy vector search), they cannot be scaled independently.
- **Blast radius**: A problem in one system (e.g., runaway compaction) affects all systems in the slate.
- **Complexity**: The combined key space must be carefully designed to avoid conflicts and maintain efficient access patterns.

**Example use case**: A usage-based billing system that captures raw usage events in a log and simultaneously updates pre-aggregated metrics in a timeseries system. Both updates would happen atomically—either the event is recorded and metrics updated, or neither happens.

```
┌─────────────────────────────────────────────┐
│                 Single Slate                │
│  ┌─────────────┐       ┌─────────────────┐  │
│  │     Log     │       │     TSDB        │  │
│  │  (events)   │       │  (aggregates)   │  │
│  └─────────────┘       └─────────────────┘  │
│                                             │
│  ─────────── Shared Write Batch ──────────  │
│  ─────────── Snapshot Isolation ──────────  │
└─────────────────────────────────────────────┘
```

#### External Composition

External composition connects data systems that reside in separate slates. Each system maintains its own storage, compaction, and scaling characteristics.

**What might this enable?**

- **Semantic alignment**: If OpenData APIs expose common semantic concepts, transformations between systems could be type-safe and validated.
- **Built-in connectors**: Data movement between systems could be handled by OpenData infrastructure rather than application code.
- **Operational consistency**: Despite separate storage, all systems already share operational tooling, metrics, and deployment patterns.

**Inherent limitations:**

- **No transactional boundary**: Writes to different slates are not atomic.
- **Eventual consistency**: Data propagation between systems has inherent latency.

The tradeoff is independent scaling and failure isolation at the cost of consistency complexity.

**Example use case**: A log captures raw events, and a separate timeseries system maintains aggregated metrics. Imagine a built-in connector that subscribes to the log and updates the timeseries system—the user configures the composition declaratively, no custom code required.

```
┌─────────────────┐         ┌─────────────────┐
│   Slate A       │         │   Slate B       │
│  ┌───────────┐  │         │  ┌───────────┐  │
│  │    Log    │  │ ──────► │  │   TSDB    │  │
│  │  (events) │  │ connector│  │(aggregates)│  │
│  └───────────┘  │         │  └───────────┘  │
└─────────────────┘         └─────────────────┘
        │                           │
        └───── Common Semantics ────┘
        └───── Common Tooling ──────┘
```

### Comparison

| Aspect | Internal | External |
|--------|----------|----------|
| Transactional writes | Yes | No |
| Snapshot isolation | Yes | No (eventual) |
| Independent scaling | No | Yes |
| Failure isolation | No | Yes |
| Operational complexity | Lower | Higher |
| Configuration | Single slate | Multiple slates + connectors |

### SlateDB as a Composition Foundation

SlateDB isn't just a convenient shared storage layer—its specific capabilities enable composition patterns that would be difficult or impossible with traditional storage engines.

**Arbitrary checkpoints**: SlateDB can create checkpoints at any point, capturing a consistent snapshot of the LSM tree. This is fundamental to composition because it provides well-defined points for coordination. A checkpoint represents "the state of this system at this moment"—a concept that composition can build on.

**Cheap clones**: SlateDB checkpoints are cheap to create and maintain because they share underlying SST files. This opens possibilities that traditional databases can't offer:
- Create a checkpoint before a risky transformation, roll back if needed
- Fork a composed system for testing or experimentation
- Maintain multiple views of the same data at different points in time

**Cross-system snapshot semantics**: If OpenData understands the composition graph—which systems exist and how data flows between them—it could provide snapshot semantics that span systems:

- **Internal composition**: Cross-system snapshots come for free. All systems share a single SlateDB instance, so a SlateDB checkpoint captures consistent state across all of them.

- **External composition**: Cross-system snapshots require coordination. Since systems reside in separate slates, we'd need distributed snapshot algorithms. Chandy-Lamport is the classic approach: inject markers into the data flow, have each system checkpoint when it sees the marker. Because SlateDB checkpoints are cheap and fast, this becomes practical.

**What this might enable:**

- **Point-in-time recovery across systems**: Restore a composed application to a consistent state, even if the systems are externally composed.
- **Consistent backups**: Capture the state of Log + TSDB + Vector at a single logical instant.
- **Time travel queries**: Query the composed system as it existed at a previous point in time.
- **Safe experimentation**: Clone the entire composition, run experiments, discard or promote the results.

**Open questions:**

- What would the API look like for cross-system checkpoints? How does a user request "snapshot everything"?
- For external composition, how do we handle in-flight data during Chandy-Lamport? What are the consistency semantics?
- How do cheap clones interact with composition? Can you clone just one system in a composition, or must you clone all of them?

### Semantic Foundations

What semantic foundations would enable principled composition? Some concepts that might matter:

- **Record identity**: Well-defined keys could enable deterministic routing and conflict-free key spaces.
- **Time semantics**: Common notions of event time and ingestion time could enable time-based correlation and consistent windowing.
- **Schema alignment**: Shared schema conventions could enable type-safe transformations that survive schema evolution.

What would APIs need to expose to make these semantic connections useful?

### Embedding and Ingest Flexibility

Traditional systems have rigid ingest APIs—Kafka's producer, Prometheus's OTLP endpoint. Composing them means operating multiple services, each with its own endpoints, auth, and monitoring. Each boundary adds latency, operational burden, and cost.

```
┌──────────┐       ┌──────────┐       ┌───────────┐       ┌────────────┐
│ Producer │ ────► │  Kafka   │ ────► │ Connector │ ────► │ Prometheus │
└──────────┘       └──────────┘       └───────────┘       └────────────┘
                        │                   │                   │
                   Endpoints           Endpoints            Endpoints
                   Auth/TLS            Auth/TLS             Auth/TLS
                   Monitoring          Monitoring           Monitoring
                   Scaling             Scaling              Scaling
```

Three independent systems to operate, each with its own failure modes and management plane.

**OpenData's library-first architecture suggests a different model.** Because systems are embeddable libraries, composition could happen at the library level:

```
┌─────────────────────────────────────────┐
│           Embedded Composition          │
│                                         │
│  ┌─────────────┐     ┌───────────────┐  │
│  │ Log Reader  │ ──► │  TSDB Writer  │  │
│  │ (embedded)  │     │  (embedded)   │  │
│  └─────────────┘     └───────────────┘  │
│         │                    │          │
│         └── Object Store ────┘          │
└─────────────────────────────────────────┘
            │
            ▼
      ┌──────────┐
      │ Grafana  │  (only external endpoint needed)
      └──────────┘
```

If telemetry only comes from a log, the user could embed the Log reader directly with the TSDB writer—no intermediate endpoints, no RPC, no auth, no connector service. The only endpoint needed is downstream for queries (e.g., Grafana).

**Potential benefits**: Fewer network hops, simplified operations (one process), reduced cost (no intermediate services), simpler security (no intermediate auth boundaries).

Embedding is a deployment choice within external composition—it doesn't change the consistency semantics, but could eliminate the operational overhead that typically accompanies integration.

**Open question**: What API design would make embedding natural?

### Next Steps

This RFC establishes terminology and sketches the landscape. If there's interest in pursuing this direction, follow-up work might include:

1. **Concrete use case exploration**: Pick a specific composition (e.g., Log→TSDB) and prototype what the API and deployment model could look like.
2. **Semantic foundation design**: Determine what common concepts APIs should expose to enable composition.
3. **Connector framework**: Design how data flows between externally composed systems.
4. **Internal composition feasibility**: Investigate what changes would be needed to support multiple systems in a single slate.

## Open Questions

1. **Use cases**: What are the most compelling composition patterns? Log→TSDB? Log→Vector? What real problems would this solve?

2. **Hybrid modes**: Should we support internal and external composition together (e.g., two systems internally composed, externally composed with a third)?

## Updates

| Date       | Description |
|------------|-------------|
| 2026-01-28 | Initial draft |
