# Vector Quickstart

This is a hands-on quickstart for the OpenData vector database. The quickstart runs a vector db instance and loads
two books, indexing them for both semantic search and attribute filtering. Then, it shows how to query the vector
db three different ways: directly query the HTTP writer, query a separate HTTP reader process, and query using an
embedded Rust reader. The separate HTTP reader and embedded reader are fully decoupled from the writer and read
object storage directly.

## What's Inside

**Dataset:** ~330 documents from two books:
- **The Rust Programming Language** (~80 sections from the official Rust book)
- **The Up-To-Date Sandwich Book** (~250 sandwich recipes from Project Gutenberg)

**Architecture:**
```
docker compose up
  ├── embedding-server   (port 9000) ── Python: fastembed HTTP API for text→embedding
  ├── vector-writer      (port 8080) ── HTTP write + search server
  ├── ingestor           ── Python: embeds docs, writes to writer, runs demo searches, exits
  ├── vector-reader      (port 8081) ── HTTP read-only server (starts after ingestor)
  └── embedded-reader    ── Rust: VectorDbReader + embedding server for arbitrary queries
```

## Quick Start

```bash
cd vector/quickstart
docker compose up
```

This will:
1. Start an embedding server (fastembed with `all-MiniLM-L6-v2`, 384 dimensions) for generating text embeddings
2. Start the vector writer server
3. Run an ingestor process that embed all documents, write to the database using the writer, and serves search queries.
4. Start the vector reader server
5. Run the embedded reader with hardcoded demo queries

After completion, the writer (8080), reader (8081), and embedding server (9000) stay running.

You can look out for a message like the following to see that the environment is fully started:
```bash
embedded-reader-1   |
embedded-reader-1   |
embedded-reader-1   | Embedded reader complete!
embedded-reader-1 exited with code 0
```

## Try It Yourself

### Embed a query and search via the HTTP reader or writer

```bash
# First embed the query, then search
EMBEDDING=$(curl -s -X POST http://localhost:9000/embed \
  -H "Content-Type: application/json" \
  -d '{"texts": ["How do I work with slices?"]}' | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin)['embeddings'][0]))")

# search using the HTTP writer
curl -s -X POST http://localhost:8080/api/v1/vector/search \
  -H "Content-Type: application/protobuf+json" \
  -H "Accept: application/protobuf+json" \
  -d "{\"vector\": $EMBEDDING, \"k\": 5, \"includeFields\": [\"book\", \"chapter\", \"section\", \"text\"]}" | python3 -m json.tool

# search using the HTTP reader
curl -s -X POST http://localhost:8081/api/v1/vector/search \
  -H "Content-Type: application/protobuf+json" \
  -H "Accept: application/protobuf+json" \
  -d "{\"vector\": $EMBEDDING, \"k\": 5, \"includeFields\": [\"book\", \"chapter\", \"section\", \"text\"]}" | python3 -m json.tool
```

### Search with a metadata filter

```bash
curl -s -X POST http://localhost:8081/api/v1/vector/search \
  -H "Content-Type: application/protobuf+json" \
  -H "Accept: application/protobuf+json" \
  -d "{\"vector\": $EMBEDDING, \"k\": 5, \"includeFields\": [\"book\", \"chapter\", \"section\", \"text\"], \"filter\": {\"eq\": {\"field\": \"book\", \"value\": \"The Rust Programming Language\"}}}" \
  | python3 -m json.tool
```

### Run an arbitrary query with the embedded reader

You can use the embedded reader program to run an arbitrary search query. This program uses vector's Rust API to
load a vector DB reader that directly queries the db from object storage.

```bash
docker compose run embedded-reader \
  /config/reader.yaml http://embedding-server:9000 \
  "How does ownership work?"
```

## Regenerating Documents

The `data/documents.json` file is pre-generated and committed. To regenerate:

```bash
cd data
python3 prepare_documents.py
```

This fetches both books from the internet and chunks them into documents.

## Services

| Service | Port | Description |
|---------|------|-------------|
| `embedding-server` | 9000 | fastembed HTTP wrapper (`POST /embed`, `GET /health`) |
| `vector-writer` | 8080 | Read-write vector HTTP server |
| `vector-reader` | 8081 | Read-only vector HTTP server |
| `embedded-reader` | - | Rust binary using `VectorDbReader` directly |
| `ingestor` | - | One-shot: embeds + writes all documents, then exits |

## Configuration

- `config/writer.yaml` — Writer config: 384 dims, L2 distance, SlateDB local storage
- `config/reader.yaml` — Reader config: same storage, read-only

Both configs define four metadata fields: `book` (indexed), `chapter` (indexed), `section`, and `text`.
