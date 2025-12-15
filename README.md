# Why OpenData?

## Our Beliefs 

1. We believe that object storage is a fundamentally new ingredient in data systems: it provides highly durable, highly available, infinite storage with unique performance and cost structures. On the one hand, it solves one of the hardest problems in distributed data systems: consistent replication. On the other hand, care must be taken to make it work corretly, performantly, and cost effectively. It turns out that the techniques to achieve the latter are pretty similar across systems: batching on writes, caching on reads, snapshot isolation. When done right, systems built natively on object storage are far simpler and cheaper to operate in modern clouds than those that haven't used object storage as a starting point.
2. Inspired by the UNIX philosophy, we believe single purpose systems that compose well are superior to systems that try to solve many problems under one umbrella. Today's business landscape incentivizes the vendors who are custodians of our beloved open source systems to bloat their offerings in order to justify higher prices that can deliver required margins. This hurts the developer base at large.
3. Further, the vendors who are the custodians of today's open source projects are sadly not incentivized to modernize the open source offerings. Their business model requires them to keep cloud-native optimizations propietary to their clouds.
 
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

