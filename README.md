# Overview

<img src="https://github.com/opendata-oss/opendata/blob/main/public/github-banner.png?raw=true" alt="OpenData" width="100%">

[![Discord](https://img.shields.io/badge/discord-join-7289DA?style=flat-square&logo=discord)](https://discord.gg/CsAQJ2AJGU)
[![GitHub License](https://img.shields.io/github/license/opendata-oss/opendata?style=flat-square)](LICENSE)

OpenData is a collection of open source databases built on a common, object-native storage and infrastructure 
foundation. This shared foundation means every database has a virtually identical operational profile, which makes
our database fleet materially easier and cheaper to operate than alternatives. 

The common foundation has two distinct components:

**SlateDB as the common storage layer:** [SlateDB](https://www.slatedb.io) is an object-store-native LSM tree that
handles write batching, tiered caching, and compaction. It provides snapshot isolation via atomic manifest updates 
using S3's compare-and-set. Each individual OpenData database implements its own domain-specific data structure and 
query layer on top of SlateDB. 

**OpenData as the common infrastructure layer:** OpenData is the shared foundation for the layers above storage:
service infrastructure, service catalog,  admin tooling, distributed state infrastructure, configuration systems, 
and testing frameworks. 

Taken together, with OpenData there is only one storage engine and one set of operational tooling to learn across all
systems. By inheriting these key components, individual databases focus only on the unique query semantics, data 
layout and optimizations.

# OpenData Databases

- **[TSDB](timeseries/README.md)**: Object-store-native timeseries. Prometheus remote-write compatible.
- **[Log](log/README.md)**: Event streaming with a replayable log per key.
- **[Vector](vector/README.md)**: SPANN-style ANN search. Centroids in memory, posting lists on disk.

# Roadmap

Each database has its own roadmap, documented in their READMEs. SlateDB has its own roadmap as well. Here is what's in
the pipe for the rest of the shared foundation:

- [ ] Common service infrastructure: server runtime with pluggable protocols, shared metrics, health checks.
- [ ] Service Registry: discover and browse deployed databases.
- [ ] Admin tooling: deploy, teardown, upgrade, inspect, migrate databases.
- [ ] Benchmark and regression testing frameworks.
- [ ] Distributed mode: state sharding and request routing.

## Bigger ideas for later
- [ ] Shared ingest layer: all writes got a shared service for better batching. A compactor framework creates queryable state.
- [ ] Flexible deployment modes: embedded writers + hosted readers, fully embedded, fully distributed, etc.
- [ ] Deterministic simulation testing.

# Get Involved

We are early and building in the open, with most discussions happening on [Discord](https://discord.gg/CsAQJ2AJGU).

**Want to build?** Check out the open RFCs, these are the active design efforts. Or you can check out issues 
labeled `good-first-issue` to get coding right away. Or simply file a bug or add a feature request!

**Have opinions?** What databases should exist under OpenData? What operational problems matter most? Open 
an issue or find us on [Discord](https://discord.gg/CsAQJ2AJGU).

**Want to follow along?** Star the repo, join [Discord](https://discord.gg/CsAQJ2AJGU), or [sign the manifesto](https://www.opendata.dev/manifesto).
