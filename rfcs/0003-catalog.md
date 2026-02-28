# RFC 0003: Catalog

**Status**: Draft

**Authors**:
- [Jason Gustafson](https://github.com/hachikuji)

## Summary

This RFC proposes a catalog system for OpenData that serves as a central management plane for OpenData storage systems. The catalog provides a single point to manage metadata about the systems ("slates") a user has installed, including their names, types, and object storage configuration. The catalog itself is implemented as a slate backed by SlateDB, following the same patterns as other OpenData subsystems.

## Motivation

OpenData comprises multiple storage subsystems (log, timeseries, vector, etc.) that share a common foundation on SlateDB. As users deploy multiple instances of these systems, several operational concerns emerge that are not well-served by existing orchestration systems:

1. **Discovery** — There is no unified way to enumerate which slates exist within an environment. Operators must track this information externally or inspect object storage directly.

2. **Usage tracking** — Orchestration systems like Kubernetes know what pods are running, but not which applications are actively reading or writing to a given slate. This information lives at the storage layer.

3. **Safe deprovisioning** — Before removing a slate, operators need to know whether it has active consumers. Without usage tracking, this requires manual coordination or risks data loss.

4. **Operational tooling** — Cross-cutting tools (backup, monitoring, migration) need a way to discover and enumerate slates without subsystem-specific knowledge.

A catalog addresses these concerns by providing:

- A registry of running slates via self-registration
- Tracking of active readers and writers for each slate
- A foundation for safety checks and operational tooling

The catalog is itself a slate backed by SlateDB, ensuring it benefits from the same durability and operational characteristics as the systems it manages.

### Hypothetical Workflow

Slates and their consumers self-register when they start up. The CLI is primarily for discovery and usage tracking:

```
$ opendata slate list
NAME        TYPE        BUCKET
events      log         s3://acme-data/events
metrics     timeseries  s3://acme-data/metrics

# A new slate is provisioned and starts up, self-registering with the catalog...

$ opendata slate list
NAME        TYPE        BUCKET
events      log         s3://acme-data/events
metrics     timeseries  s3://acme-data/metrics
orders      log         s3://acme-data/orders

$ opendata slate describe orders
Name:    orders
Type:    log
Bucket:  s3://acme-data/orders
Writers: order-service
Readers: analytics, billing

# Before deprovisioning, check if it's safe to remove
$ opendata slate describe metrics
Name:    metrics
Type:    timeseries
Bucket:  s3://acme-data/metrics
Writers: telemetry-collector
Readers: dashboard, alerting
```

## Goals

- Define "slate" as the fundamental unit of an OpenData system
- Specify the metadata required to describe a slate (name, type, object storage location)
- Establish the catalog as a SlateDB-backed registry of slate metadata
- Define the self-registration process by which slates register themselves on startup
- Define the deregistration process for when slates are shut down
- Track active readers and writers for each slate
- Ensure the catalog can manage slates of all types (log, timeseries, vector)

## Non-Goals

- **Provisioning** — The catalog does not manage target state or drive provisioning. It reflects current state only. Provisioning is the responsibility of external systems (Kubernetes, Terraform, etc.).

## Design

### Communication Model

The catalog does not operate as a persistent service with a communication endpoint. Instead, all interaction with the catalog occurs through its SlateDB-backed storage. This follows directly from SlateDB's Reader/Writer model, where components temporarily assume one of two roles:

- **Writer** — A component that opens the catalog with write access to mutate state (self-register, update metadata, deregister).
- **Reader** — A component that opens a read-only view of the catalog to observe state changes.

This model enables coordination between loosely-coupled components without requiring a running service or explicit RPC. Components communicate implicitly by writing state that other components read.

#### Example: Self-Registration on Startup

The catalog sits downstream of provisioning. Each slate can be configured with an optional reference to a catalog. When a slate starts up, it registers itself, ensuring the catalog reflects the current state of running systems.

Consider a flow where a Kubernetes operator provisions a new slate:

1. A user creates a Kubernetes Custom Resource specifying a new slate.
2. The K8s operator provisions the slate with a catalog reference in its configuration.
3. When the slate starts, it assumes the **Writer** role to register itself in the catalog.
4. CLI tooling or other components can observe the catalog as **Readers** to discover running slates.

```
┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐
│   Provisioner   │         │      Slate      │         │     Catalog     │
│   (K8s, etc.)   │         │    (orders)     │         │    (SlateDB)    │
└────────┬────────┘         └────────┬────────┘         └────────┬────────┘
         │                           │                           │
    create slate with                │                           │
    catalog reference                │                           │
         │──────────────────────────►│                           │
         │                           │                           │
         │                           │  [Writer] self-register   │
         │                           │  on startup               │
         │                           │──────────────────────────►│
         │                           │                           │
         │                           │                           │
┌────────┴────────┐                  │                           │
│      CLI        │                  │                           │
└────────┬────────┘                  │                           │
         │                           │                           │
         │            [Reader] discover running slates           │
         │──────────────────────────────────────────────────────►│
         │                           │                           │
```

This approach keeps provisioning out of the catalog's scope. The catalog represents *current state*—what systems are actually running—rather than *target state*. Provisioning remains the responsibility of existing orchestration systems (Kubernetes, Terraform, manual deployment, etc.).

#### Implications

- **No always-on service** — The catalog does not require a continuously running process. Components open Reader or Writer handles as needed.
- **Distributed coordination via storage** — Object storage (S3, GCS, etc.) serves as the durable communication medium.
- **Consistency from SlateDB** — SlateDB's single-writer guarantee ensures catalog mutations are serialized. Readers see a consistent snapshot.
- **Polling for changes** — Readers must poll to observe updates. Future work may explore change notification mechanisms built on SlateDB.

_Additional design sections to be completed in a future revision._

## Alternatives

### Catalog-Driven Provisioning

An alternative approach positions the catalog as the source of truth for *target state*, with provisioning systems watching the catalog and responding to changes. A user would register a new slate in the catalog, a provisioning system (e.g., a Kubernetes operator) would observe the registration and create the necessary infrastructure, then update the catalog with provisioning status.

This approach was rejected because it duplicates functionality that provisioning systems already provide. Systems like Kubernetes have their own declarative model for specifying target state (e.g., Custom Resource Definitions). Adding a catalog-driven provisioning layer would require:

- The catalog to maintain target vs. current state, adding complexity.
- Provisioning systems to sync state back to the catalog, creating potential for drift.
- Users to learn a new provisioning model rather than using native tooling they already know.

Instead, we explicitly take provisioning out of the catalog's scope. The catalog represents *current state*—what systems actually exist—rather than *target state*. Provisioning remains the responsibility of existing orchestration systems.

## Open Questions

1. **Catalog bootstrap** — How is the catalog itself discovered? If the catalog is a slate, where is its object storage configuration stored?

2. **Bucket metadata** — What specific fields are required for object storage configuration? (bucket name, region, path prefix, credentials reference?)

3. **Catalog location** — Should there be one catalog per bucket, per region, or per "environment"? What is the deployment topology?

## Updates

| Date       | Description |
|------------|-------------|
| 2026-01-21 | Initial draft |
