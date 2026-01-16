# Overview

<img src="https://github.com/opendata-oss/opendata/blob/main/public/github-banner.png?raw=true" alt="OpenData" width="100%">

[![Discord](https://img.shields.io/badge/discord-join-7289DA?style=flat-square&logo=discord)](https://discord.gg/EUv3Bqcv)
[![GitHub License](https://img.shields.io/github/license/opendata-oss/opendata?style=flat-square)](LICENSE)

OpenData is a collection of open source databases designed from ground up for object storage. We aim to deliver highly focused, objectstore-native versions of online databases that can power your application stack. As of today, we have a competitive timeseries database and a data streaming backend. We plan to ship vector search, text search, and other database types over time. Contributions are welcome!

Building performant, cost-effective, and correct online database on object storage takes special care. Successful designs all have to solve the problem of write batching, multiple levels of caching, and snapshot isolation for correctness. OpenData databases build on a common foundation to solve these problems. This common foundation gives our databases a common set of operational tools, configuration systems, etc., that make our databases easier to operate in aggregate. 

# Databases

OpenData ships three databases today, with more on the way:

* **TSDB**: An objectstore native timeseries database that can serve as a backend for Prometheus. Its a great option for a low cost, easy to operate, grafana backend. [Learn more](timeseries/rfcs/0001-tsdb-storage.md).
* **Log**: Think of it as Kafka 2.0. An objecstore native event streaming backend that supports millions of logs, so you can finally get a replayable log per key. [Learn more](log/rfcs/0001-storage.md).
* **Vector**: An objectstore native vector database for approximate nearest neighbor search. SPANN-style architecture with centroids in memory and posting lists on disk for billion-scale capacity. [Learn more](vector/rfcs/0001-storage.md).


# Which usecases are OpenData Databases suited for?

The key feature of OpenData databases is that object storage is the sole persistence layer, and readers and writers coordinate solely via manifest files in object storage. This results in several interesting properties: 
1. Object storage being the sole persistence layer means that each Database instance can be tuned to trade off between [Latency, Cost, and Durability](https://materializedview.io/p/cloud-storage-triad-latency-cost-durability). This flexibility allows new workloads which may not have been economical with traditional designs.
2. Since readers and writers are stateless and decoupled, each can be scaled to 0 independently. This means workloads with massive skews between writes and reads can be served far more economically with OpenData databases.
3. The architecture allows several deployment models. It's possible for OpenData database components to be fully embedded in the application process. Or they can be fully distributed, with each component running as services in a k8s cluster. In either case, data is in S3 and always persistent. This makes per app, per agent, or other arrangements a natural fit.

The flip side of this decoupled architecture is that you have higher end-to-end latency between when data is inserted into the system and when it is returned in a query. This means truly interactive use cases where users must read their writes as soon as possible are not good fits for OpenData databases. However, when some end-to-end latency is acceptable, the flexiblity of the OpenData architecture makes OpenData databases the superior option in a cloud-native world. 

# Quick Start

* [TSDB](./timeseries/README.md#quickstart)
* Log: TODO
* Vector: TODO


# Architecture

## 10,000ft view

The OpenData ecosystem builds databases for object storage on a common core. At a high level, the ingestion, storage, and query layers are logically separated from each other. The ingestion layer implements write APIs, the storage layer takes writes and indexes them into read optimized data structures, and the query layer implements query APIs that consume read optimized versions of the written data. Each layer leverages a common set of components, which is a key part of what makes the experience to develop and operate OpenData databases efficient.

```
  ┌───────┐        ┌───────┐        ┌───────┐
  │Ingest │        │Storage│        │ Query │
┌─┴───────┴─┐    ┌─┴───────┴─┐    ┌─┴───────┴─┐
│    OTEL   │    │    TSDB   │    │   PromQL  │
│           │    │           │    │           │
├───────────┤    ├───────────┤    ├───────────┤
│   Kafka   │    │    Log    │    │   Kafka   │
│ Producer  │    │           │    │  Consumer │
├───────────┤    ├───────────┤    ├───────────┤
│  Object   │    │  Search   │    │   Lucene  │
│  storage  │    │           │    │     QL    │
└───────────┘    └───────────┘    └───────────┘
      ╔════════════════════════════════╗
      ║        Common Components       ║
      ║         (SlateDB, etc)         ║
      ╠════════════════════════════════╣
      ║        Object Storage          ║
      ║                                ║
      ╚════════════════════════════════╝
```

Next, we dig into the typical data flow for a specific OpenData database.  

## Typical Data Flow

Each OpenData databases has a high level data flow that looks like this:

```

 write │                          ▲ query
       │                          │
       │                          │
┌──────▼──────────────────────────┴───────┐
│                  API                    │
└─────┬─────────────────────────────▲─────┘
     1│                            6│
╔═════▼═════╗  ╔═══════════╗  ╔═════╩═════╗
║           ║  ║           ║  ║   query   ║
║ ingestors ║  ║ compactors║  ║ executors ║
║           ║  ║           ║  ║           ║
╚═════╦═════╝  ╚══▲══════▲═╝  ╚═════▲═════╝
     2│          3│     4│         5│
┏━━━━━▼━━━━━━━━━━━┻━━┳━━━▼━━━━━━━━━━┻━━━━━┓
┃        WALs        │   Collections      ┃
┣────────────────────┴────────────────────┫
┃                Metadata                 ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
              Object Storage
```
1. The API layer is specific to each database, and implements the appropriate read and write APIs for the database. When the API layer receives write requests, they are forwarded to the Ingestor component. For instance, a TSDB may implement the OTEL protocol at the API layer which receives OTEL write requests.
2. The Ingestors receive writes coming from the API layer, convert them to Write-Ahead-Log (WAL) records, batch these records and flush them to object storage, and then update the Metadata. The WAL record formats may differ for different database types. We describe the metadata layer in detail later, but for now it suffices to know that the metadata contains locations and versions of all files in the system. The metadata lives in object storage, and all OpenData systems use the same components to enable writing metadata updates atomically. Atomic updates to metadata ensure that readers and writers in the system operate on consistent views of the data in the system. 
3. There are two types of Compactors in the OpenData system. One type is called the WAL compactor. It takes new WAL files written by ingestors and write the WAL entries into a format that's optimized to serve the queries of each database. For example, a compactor for a TSDB will take a WAL file and write inverted indexes, dictionaries, etc. These read-optimized files are called Collections. 
4. The other type of Compactor takes existing Collections and rewrites them into new Collections. These Compactors are used to drop deleted data, to improve data locality, etc. The logic of the Compactor is database specific. The goal of the background compaction process is to maintain read optimized versions of the data across updates. 
5. The query executors are responsible for serving queries on the system. These are custom for database. For instance, the query executor of our TSDB implements PromQL. When executing a query, the executors lookup the metadata to find the files relevant to serving the query, and retrieves the data from those files. The main job of the query executor is to keep the right indexes in memory and the hot data cached so that queries on the system are both fast and do not create unduly expensive calls to the objectstore.
6. The API layer forwards queries to the query executors for compliation and execution, and returns results to clients. 


## Shared Components

### Storage

OpenData leans heavily on [SlateDB](https://www.slatedb.io) as the core data substrate. We will continue to invest in SlateDB to solve the storage problems that are common across OpenData databases. Here are some ideas for how we can leverage SlateDB as we build out OpenData.

1. The Ingestors are likely to be very similar across databases, since they just write a log to satisfy durability guarantees. SlateDB already has a WAL implementation that is optimized for writing to object storage. It makes sense for us to reuse that WAL implementation while extending and generalizing it wherever necessary.
2. SlateDB already has compactor modules. Generalizing the compactor framework so that custom compaction logic and output formats can be plugged in by database implementations is a logical direction for us to take. 
3. Further improvements would be to enable parallelizing compaction, which would mean developing some notion of partitioning into SlateDB. 
5. SlateDB already has an objectstore native implementation of an LSM tree. This implementation is built to cache index data to enable fast lookups, etc. This generic structure is likely to be useful across a variety of database types. If there are other data structures that are useful across databases (like high performance dictionaries, etc), we'd want to make them available in SlateDB.

### Query

Storage is one third of the problem a database needs to solve, with querying being another third. Thus we'd like to have a shared framework for querying like we have SlateDB for storage. We are evaluating [Apache Datafusion](https://datafusion.apache.org/) for this purpose, but suggestions for better options are welcome! 


### Metadata 

Managing metadata is the final third of the problem that needs to be solved by a database. The metadata problem boils down to making sure that data  stored by the ingestion layer can be found and queried by the query engine, and that the writers and readers are coordinated so that the Integrity of writes (I in ACID) is maintained. 

In addition to solving core storage problems, SlateDB also solves this basic metadata problem via [Manifests](https://github.com/slatedb/slatedb/blob/main/rfcs/0001-manifest.md). It's worth reading that RFC, but here's a summary of manifests work and why we can build on them to solve the metadata problem for OpenData.

1. The manifest contains the locations and versions of all the data files the system. 
2. The general principle is that data is only read if the files which contain that data are accessible via the manifest. 
3. SlateDB leverages compare-and-set operations that are available in every object store to atomically update the manifest.
4. Ingestors and Compactors flush data to objectstore first, and then atomically update the locations of the new files in the manifest.
5. SlateDB leverages the version information in the manifest to ensure that versions can't roll back by rejectiing writes with older versions. This provides a fencing mechansism which ensures zombie writers can't roll data backward. 
6. Readers of data, including Compactors and Query Executors, thus always get a consistent view of the metadata at any point in time. 
7. The manifest system as a whole provides snapshot isolation across reads and writes for every OpenData database. 

Different databases likely need different metadata. Making the manifest system extensible would allow us to use it across databases, which we think is a high leverage thing to do.

# Why OpenData?

1. We believe that object storage is a fundamentally new ingredient in data systems: it provides highly durable, highly available, infinite storage with unique performance and cost structures. It solves one of the hardest problems in distributed data systems: consistent replication. At the same time, tremendous care must be taken to make object storage work correctly, performantly, and cost-effectively. When done right, systems built natively on object storage are far simpler and cheaper to operate in modern clouds than the alternatives. We want to bring the benefits of object storage to every database.
2. Inspired by the UNIX philosophy, we believe single purpose systems that compose well are superior to systems that try to solve many problems under one umbrella, which is what has tended to happen with existing open surce projects since each is developed in a silo. Buy building multiple focused databases on a common core, OpenData will not have to trade off power for simplicity. 
3. Object store native designs have tended to be proprietary, to the detriment of the developer community at large. OpenData remedies that by being MIT licensed from day one. 

