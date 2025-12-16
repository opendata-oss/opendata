# Overview

OpenData is a collection of open source data infrastructure projects designed from ground up objectstoraege. Our goal is to deliver highly focused, objectstore-native versions of online databases that can power your application stack. As of today, we have a competitive timeseries database and a data streaming backend. We plan to ship vector search, text search, and other database types over time. Contributions are welcome!

Building performant, cost effective, and correct online systems on objectstorage takes special care. Successful designs all have to solve the problem of write batching, multiple levels of caching, and snapshot isolation for correctness. OpenData systems build on a common foundation to solve these problems. This common foundation gives OpenData systems a common set of operational tools, configuration systems, etc., that make our systems easier to operate in aggregate. 


# Our systems.

OpenData ships two systems today:

* TSDB : An objectstore native timeseries database that can serve as a backend for Prometheus. Its a great option for a low cost, easy to operate, grafana backend.
* EventStore: Think of it as Kafka 2.0. An objecstore native event streaming backend that allows you to have millions of logs, so you can finally get a replayable log per key cost effectively.

# Quick Start

TODO.


# Why OpenData?

## Our Beliefs 

1. We believe that object storage is a fundamentally new ingredient in data systems: it provides highly durable, highly available, infinite storage with unique performance and cost structures. On the one hand, it solves one of the hardest problems in distributed data systems: consistent replication. On the other hand, care must be taken to make it work corretly, performantly, and cost effectively. It turns out that the techniques to achieve the latter are pretty similar across systems: batching on writes, caching on reads, snapshot isolation. When done right, systems built natively on object storage are far simpler and cheaper to operate in modern clouds than those that haven't used object storage as a starting point.
2. Inspired by the UNIX philosophy, we believe single purpose systems that compose well are superior to systems that try to solve many problems under one umbrella.
3. We believe that there is a general lack of objectstore native open source data systems, to the detriment of the developer community at large. 

## Our Vision

OpenData is a solution to these sturctural problems with open source data infrastructure. 

OpenData is an open source community building focused and object-store native versions of every online data system from event streaming systems, to time series databases, text search systems, vector databases, and beyond. These systems will embody the following principles: 

1. Each OpenData product will target one core use case and aim to be great at it. 
2. Each OpenData product will be built from ground up with object storage as a starting point.
3. Each OpenData product will be built on a common set of primitives. One set of primitives is [SlateDB](https://www.slatedb.io), but there may be others. 
4. Each product and every common component will be licensed under the MIT license. 
5. All OpenData products will be built with a uniform operational experience, which includes common CLI tooling, common versioning schemes, common configuration styles, and more.
6. Every OpenData product will be designed to operate with the others. 

OpenData aims to put control back in the hands of developers. Your data is always in your custody, and OpenData makes it practical to serve that data to any app without needing multiple specialist engineers on staff.

# License

MIT License
