"""Ingestor: embeds documents via the embedding server and writes them to the vector writer."""

import json
import os
import sys
import time

import requests

WRITER_URL = os.environ.get("WRITER_URL", "http://localhost:8080")
EMBEDDING_URL = os.environ.get("EMBEDDING_URL", "http://localhost:9000")
EMBED_BATCH_SIZE = 50
WRITE_BATCH_SIZE = 20


def wait_for_service(url, name, max_retries=60, delay=2):
    for i in range(max_retries):
        try:
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                print(f"  {name} is ready")
                return
        except requests.ConnectionError:
            pass
        if i % 10 == 0:
            print(f"  Waiting for {name}...")
        time.sleep(delay)
    print(f"ERROR: {name} did not become ready after {max_retries * delay}s")
    sys.exit(1)


def embed_texts(texts):
    """Call the embedding server to embed a list of texts."""
    resp = requests.post(
        f"{EMBEDDING_URL}/embed",
        json={"texts": texts},
        timeout=120,
    )
    resp.raise_for_status()
    return resp.json()["embeddings"]


def write_vectors(vectors):
    """Write a batch of vectors to the writer server."""
    payload = {
        "upsertVectors": [
            {
                "id": v["id"],
                "attributes": {
                    "vector": v["embedding"],
                    "book": v["metadata"]["book"],
                    "chapter": v["metadata"]["chapter"],
                    "section": v["metadata"]["section"],
                    "text": v["text"],
                },
            }
            for v in vectors
        ]
    }
    resp = requests.post(
        f"{WRITER_URL}/api/v1/vector/write",
        headers={"Content-Type": "application/protobuf+json"},
        json=payload,
        timeout=30,
    )
    if not resp.ok:
        print(f"  ERROR: {resp.status_code} {resp.text}", flush=True)
    resp.raise_for_status()
    result = resp.json()
    return result.get("vectorsUpserted", 0)


def search_writer(query_text, k=5, filter_obj=None, include_fields=None):
    """Embed a query and search via the writer server."""
    embedding = embed_texts([query_text])[0]
    payload = {"vector": embedding, "k": k}
    if filter_obj:
        payload["filter"] = filter_obj
    if include_fields is not None:
        payload["includeFields"] = include_fields
    resp = requests.post(
        f"{WRITER_URL}/api/v1/vector/search",
        headers={
            "Content-Type": "application/protobuf+json",
            "Accept": "application/protobuf+json",
        },
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def print_results(title, results):
    """Pretty-print search results."""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")
    for i, r in enumerate(results.get("results", []), 1):
        vec = r.get("vector", {})
        attrs = vec.get("attributes", {})
        score = r.get("score", 0)
        book = attrs.get("book", "?")
        section = attrs.get("section", "?")
        text_preview = (attrs.get("text", "")[:100] + "...") if attrs.get("text") else "?"
        print(f"  {i}. [{book}] {section}  (score: {score:.4f})")
        print(f"     {text_preview}")
    print()


def main():
    print("=" * 70)
    print("  Vector Quickstart Ingestor")
    print("=" * 70)

    # Wait for services
    print("\nWaiting for services...")
    wait_for_service(f"{EMBEDDING_URL}/health", "Embedding server")
    wait_for_service(f"{WRITER_URL}/-/ready", "Vector writer")

    # Load documents
    print("\nLoading documents...")
    with open("documents.json") as f:
        documents = json.load(f)
    print(f"  Loaded {len(documents)} documents")

    # Count by book
    book_counts = {}
    for doc in documents:
        book = doc["metadata"]["book"]
        book_counts[book] = book_counts.get(book, 0) + 1
    for book, count in book_counts.items():
        print(f"  - {book}: {count} documents")

    # Embed all documents
    print("\nEmbedding documents...")
    all_texts = [doc["text"] for doc in documents]
    all_embeddings = []
    for i in range(0, len(all_texts), EMBED_BATCH_SIZE):
        batch = all_texts[i : i + EMBED_BATCH_SIZE]
        embeddings = embed_texts(batch)
        all_embeddings.extend(embeddings)
        print(f"  Embedded {min(i + EMBED_BATCH_SIZE, len(all_texts))}/{len(all_texts)}")

    # Attach embeddings to documents
    for doc, emb in zip(documents, all_embeddings):
        doc["embedding"] = emb

    # Write to vector database
    print("\nWriting vectors to database...")
    total_written = 0
    for i in range(0, len(documents), WRITE_BATCH_SIZE):
        batch = documents[i : i + WRITE_BATCH_SIZE]
        count = write_vectors(batch)
        total_written += count
        print(f"  Written {total_written}/{len(documents)}")

    # Wait for flush
    print("\nWaiting 10s for writer to flush data...")
    time.sleep(10)

    # Demo searches
    print("\n" + "=" * 70)
    print("  Running Demo Searches")
    print("=" * 70)

    projection = ["book", "chapter", "section", "text"]

    # 1. Unfiltered search
    results = search_writer("How do I handle errors in my program?", include_fields=projection)
    print_results('Search: "How do I handle errors in my program?"', results)

    # 2. Filtered: Rust book only
    results = search_writer(
        "ownership and borrowing",
        filter_obj={"eq": {"field": "book", "value": "The Rust Programming Language"}},
        include_fields=projection,
    )
    print_results('Search: "ownership and borrowing" (Rust book only)', results)

    # 3. Filtered: Sandwich book only
    results = search_writer(
        "cheese sandwich recipes",
        filter_obj={"eq": {"field": "book", "value": "The Sandwich Book"}},
        include_fields=projection,
    )
    print_results('Search: "cheese sandwich recipes" (Sandwich book only)', results)

    # 4. Cross-domain search
    results = search_writer("slicing and cutting", include_fields=projection)
    print_results('Search: "slicing and cutting" (both books)', results)

    # Print curl examples
    print("=" * 70)
    print("  Try it yourself!")
    print("=" * 70)
    print()
    print("  The writer (port 8080) and reader (port 8081) servers are still running.")
    print("  The embedding server (port 9000) is also available.")
    print()
    print("  Embed a query:")
    print('    curl -s -X POST http://localhost:9000/embed \\')
    print('      -H "Content-Type: application/json" \\')
    print("      -d '{\"texts\": [\"How does ownership work?\"]}'")
    print()
    print("  Search via the reader:")
    print('    curl -s -X POST http://localhost:8081/api/v1/vector/search \\')
    print('      -H "Content-Type: application/protobuf+json" \\')
    print('      -H "Accept: application/protobuf+json" \\')
    print("      -d '{\"vector\": [<your_embedding>], \"k\": 5}'")
    print()
    print("  Run an arbitrary query with the embedded reader:")
    print('    docker compose run embedded-reader \\')
    print('      /config/reader.yaml http://embedding-server:9000 \\')
    print('      "How does memory safety work?"')
    print()

    print("Ingestor complete!")


if __name__ == "__main__":
    main()
