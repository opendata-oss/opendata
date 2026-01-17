# Overview

<img src="https://github.com/opendata-oss/opendata/blob/main/public/github-banner.png?raw=true" alt="OpenData" width="100%">

[![Discord](https://img.shields.io/badge/discord-join-7289DA?style=flat-square&logo=discord)](https://discord.gg/EUv3Bqcv)
[![GitHub License](https://img.shields.io/github/license/opendata-oss/opendata?style=flat-square)](LICENSE)

# The Problem we are solving

   * Typical progression of a database stack: everything is postgres. But every successfull company eventually outgrows postgres. You start adding timeseries, log, vector, search, and other databases. Each of these databases have to solve very similar problems, from data storage replication, metadata management, control plane operations, administrative tooling, test tooling, etc. And so each of these databases has their own unique operational challeges. You end up either paying lots of people to operate these databases in your company, or you pay a vendor a premium to manage them for you. And these vendors aim for 80% margins, and bloat their software to justify these margins. No matter how you slice it, your data stack gets expensive. 
   * OpenData is solving this problem by building each of these databases on a shared foundation, from the core storage, to the control plane, to administrative controls, test tools, and beyond. This makes every OpenData database focused on well defined use cases, and all of them are similar to develop, operate, and tune. No need to hire an army of engineers to keep your databases online. One will do. The costs of running your data stack will fall. 

# How is Open Data solving this problem

   * OpenData relies heavily on SlateDB as the common foundation for storage. Our databases can be thought of as data structures implemented on top of SlateDB's object store native LSM tree. SlateDB provides the core storage engine that batches and caches data on top of ojbect storage. It also provides a metadata management layer which provides basic snapshot isolation to OpenData databases.
   * Similarly, OpenData databases have shared foundations for deployment runtimes, admin tooling, service registry, test tooling, and more. The roadmap section below has more information.
   * By leveraging common foundatiotns and in many cases identical solutions for major parts of the architecture of each database, each database can focus on doing one job well, and a group of OpenData Databases are remarkably easy to operate without needing specialized knowledge of any particular database. 

# Our Roadmap

## Our Databases

Each Opendata database is responsible for its own data layout on top of slateDB, its query implementation, and metadata tracking. Each database has its own roadmp for its specific features along these dimensions. Check out our databases to learn more about their design, how to use it, and what's coming up next.

## Shared Foundations

Here are the common layers we need to build across every database: 

[] Common service infrastructure: how to make every OpenData DB a service accessed over the network. this includes things like pluggable protocol support, shared metrics infrastructure, standardized health checks, extensible API routing, etc. etc. The current RFC does this well.
[] A common solution for distributed mode. If DBs are shared, how to route requests to the right shard, etc.
[] A common 'registry': How to browse your deployed DBs, access them, etc. 
[] Common admin tooling: deploy, tear down, roll, upgrade, inspect, move, etc.
[] Benchmark tooling
[] Regression testing framework: run a fixed set of queries across db versions to catch functional regressions.
[] Deterministic simulation testing. 

Some of the other bigger ideas that we are thinking of include: 

[] A shared ingest layer: writes hit a product-specific api, but then get persisted and indexed and made queryable using a standard layer that's pluggable. Here is where a shared WAL, background compactors, etc. fit in. 
[] A common way to run DBs multiple modes: embedded writers and readers, embedded writes hosted readers, etc.

# How to get involved

We are just getting started with OpenData. We'd love to hear from, whether you'd like to help build OpenData, or if you have feature requests, or whether you have entire databases you wish existed under the OpenData umbrella.. let us know! hop on our discord or file an issue.



