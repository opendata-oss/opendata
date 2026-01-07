# Vector

An objectstore-native vector database for approximate nearest neighbor (ANN) search. Built on SlateDB with a SPANN-style architecture: cluster centroids in memory for fast navigation, posting lists and vector data on disk for billion-scale capacity.

## Features

- **SPANN index**: Memory-efficient ANN search with configurable centroid ratio
- **Metadata filtering**: Inverted indexes for hybrid queries combining similarity and predicates
- **Incremental updates**: LIRE-style rebalancing maintains index quality without global rebuilds
- **Roaring bitmaps**: Compressed posting lists for fast set operations

## Design

See [rfcs/0001-storage.md](rfcs/0001-storage.md) for the storage design.
