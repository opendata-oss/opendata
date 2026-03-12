# Vector

An objectstore-native vector database for approximate nearest neighbor (ANN) search. Built on SlateDB with a SPANN-style architecture: cluster centroids in memory for fast navigation, posting lists and vector data on disk for billion-scale capacity.

## Features

- **SPANN index**: Memory-efficient ANN search with configurable centroid ratio
- **Metadata filtering**: Inverted indexes for hybrid queries combining similarity and predicates
- **Incremental updates**: LIRE-style rebalancing maintains index quality without global rebuilds
- **Roaring bitmaps**: Compressed posting lists for fast set operations

## Design

See [rfcs/0001-storage.md](rfcs/0001-storage.md) for the storage design.

## Docker

Build the image from the workspace root:

```bash
docker build -t opendata-vector -f vector/Dockerfile .
```

The container defaults to running the read-write HTTP server with config mounted at
`/config/vector.yaml`:

```bash
docker run --rm \
  -p 8080:8080 \
  -v "$(pwd)/vector.yaml:/config/vector.yaml:ro" \
  opendata-vector
```

To run the read-only server instead, override the default command:

```bash
docker run --rm \
  -p 8080:8080 \
  -v "$(pwd)/reader.yaml:/config/reader.yaml:ro" \
  opendata-vector reader --config /config/reader.yaml
```
