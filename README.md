# Overview

<img src="https://github.com/opendata-oss/opendata/blob/main/public/github-banner.png?raw=true" alt="OpenData" width="100%">

[![Discord](https://img.shields.io/badge/discord-join-7289DA?style=flat-square&logo=discord)](https://discord.gg/EUv3Bqcv)
[![GitHub License](https://img.shields.io/github/license/opendata-oss/opendata?style=flat-square)](LICENSE)

OpenData is a collection of databases built on a shared foundation. Same storage layer, same operational patterns, same tooling. The goal: make running multiple specialized databases feel like running one system.

## The Problem OpenData is solving

When you're starting out, all your data fits in Postgres. Operations are simple. You have one database to learn, one to operate, one to tune.

Then you hit scale. You need timeseries for metrics, a log store for events, vector search for embeddings. Postgres can technically do all of these, but not well at scale. So you unbundle and fall off an operational cliff.

Each new database has its own storage engine, its own replication story, its own operational quirks. Suddenly you're hiring specialists or paying vendors 80% margins to manage complexity that didn't exist when it was "just Postgres." The cost isn't in the software. It's in the operations.

OpenData is building a middle ground. Our databases may not as simple to operate as as single-node Postgres, but they are nowhere near the complexity of running five unrelated distributed databases. OpenData databases are all built on a shared foundation that lets you add capabilities without multiplying operational burden.

## The OpenData Approach

Two things make OpenData work:

**SlateDB as the storage substrate.** [SlateDB](https://www.slatedb.io) is an object-store-native LSM tree that handles write batching, tiered caching, and compaction. It provides snapshot isolation via atomic manifest updates using S3's compare-and-set. Each OpenData database is essentially a data structure built on top of SlateDB—defining its own data layout and query implementation while inheriting the storage fundamentals.

This alone is massive leverage. Tuning and configuring storage is where a bulk of database operational complexity lives. Buffer pools, write-ahead logs, compaction strategies, cache hierarchies, replication—each database handles these differently, and each requires specialized knowledge to operate well. With OpenData, there's exactly one storage system to understand. One set of knobs. One tuning methodology.

**OpenData as the operational layer.** OpenData extends that same leverage to everything above storage: deployment patterns, admin tooling (deploy, upgrade, rollback, inspect), service infrastructure (health checks, metrics, protocol handling), configuration systems, and testing frameworks. When every database deploys the same way, scales the same way, and exposes the same admin interface, you stop needing a specialist for each one.

The combination is a unified storage and operational stack. Individual databases focus only on what makes them unique—query semantics, data layout, use-case optimizations—while inheriting everything else.

## Why Now

Object storage has been around for years, but two recent S3 primitives make this architecture viable:

- **Compare-and-set**: Enables atomic metadata updates without external coordination. This is how SlateDB provides snapshot isolation—manifest updates are atomic, so readers and writers stay consistent without a separate lock service.
- **Express One Zone**: Drops latency enough that object-store-native databases can serve workloads that previously required local disk.

SlateDB is capitalizing on these primitives to build a correct, performant storage substrate. OpenData is building on SlateDB to deliver operational leverage across multiple databases.

## Tradeoffs

OpenData databases sit between OLTP (Postgres) and analytics (ClickHouse, Snowflake). Object storage is the sole persistence layer, which means:

- Higher end-to-end latency between writes and reads (seconds, not milliseconds)
- Not suitable for workloads where users must read their own writes immediately

But if your workload can tolerate that latency—and many can—you get meaningful flexibility: tune cost/latency/durability per workload, scale readers and writers independently (including to zero), and choose your deployment model (embedded, distributed, or hybrid).

## Databases

Three databases today, more coming:

- **[TSDB](timeseries/rfcs/0001-tsdb-storage.md)**: Object-store-native timeseries. Prometheus remote-write compatible.
- **[Log](log/rfcs/0001-storage.md)**: Event streaming with millions of partitions. Replayable log per key.
- **[Vector](vector/rfcs/0001-storage.md)**: SPANN-style ANN search. Centroids in memory, posting lists on disk.

## Roadmap

### Shared Infrastructure (now)
- [ ] Common service layer: pluggable protocols, shared metrics, health checks, routing
- [ ] Distributed mode: sharding and request routing
- [ ] Registry: discover and browse deployed databases
- [ ] Admin tooling: deploy, teardown, upgrade, inspect, migrate
- [ ] Benchmark and regression testing frameworks

### Bigger Ideas (later)
- [ ] Shared ingest layer with common WAL and compaction
- [ ] Flexible deployment modes: embedded writers + hosted readers, fully embedded, fully distributed
- [ ] Deterministic simulation testing

## Get Involved

We're early and building in the open.

**Want to build?** Check out the open RFCs, these are the active design efforts. Or you can check out issues labeled `good-first-issue` to get codin right away. Or simply file a bug or add a feature request!

**Have opinions?** What databases should exist under OpenData? What operational problems matter most? Open an issue or find us on [Discord](https://discord.gg/EUv3Bqcv).

**Want to follow along?** Star the repo, join [Discord](https://discord.gg/EUv3Bqcv), or [sign the manifesto](https://www.opendata.dev/manifesto).


