# Prompt: Implement RFC-0006 Text Search Milestone 0

You are an RLM/wiggum-loop coding system working in the OpenData repository. Your job is to implement the first text-search milestone for `opendata-vector`.

Assumption: "milestone one" in the request means the first implementation milestone to run in this prompt, which is RFC Milestone 0. Do not continue into RFC Milestone 1.

## Required Reading

Before changing code, read:

- `AGENTS.md`
- `CONTRIBUTING.md`
- `vector/AGENTS.md`
- `vector/rfcs/0006-text-search.md`
- The current vector write path under `vector/src/write/indexer/tree/`
- The current vector query path under `vector/src/query_engine.rs`
- The current vector model/schema/serde/storage modules under `vector/src/model.rs`, `vector/src/serde/`, and `vector/src/storage/`

Follow the repo conventions:

- Use `rg` for codebase searches. `rg` is the ripgrep command-line tool, for example `rg "SearchIndexDelta" vector/src`.
- Keep imports at module scope.
- Use `bytes::Bytes` and `bytes::BytesMut` for encoding buffers.
- Use given/when/then test structure and `should_...` test names.
- Run `cargo fmt` after Rust edits.

## Scope

Implement RFC-0006 Milestone 0 only:

- Add storage formats for FTS postings and statistics.
- Update the vector Indexer to emit FTS postings and statistics updates for inserted text fields.
- Implement simple BM25 scoring on the query path.
- Start with an end-to-end test under `vector/tests` that writes documents with text fields and searches them with BM25.
- Keep iterating until that end-to-end test passes.

The implementation target is insert-only FTS plus simple query scoring. Existing vector insert/query behavior must keep working.

## Explicit Non-Goals

Do not implement any of the following in this loop:

- Segmentation.
- Adding a segment byte to keys.
- Moving FTS data to a dedicated segment.
- Delete handling through an FTS deletions bitmap.
- Custom compaction policy.
- Compaction filters.
- Major/full compaction behavior for FTS.
- MaxScore, BlockMaxScore, impacts, SIMD scoring, or query-path optimization beyond straightforward BM25.
- Compatibility or migration logic for older storage formats.
- Hybrid ANN + BM25 search.

Vector is pre-1.0. It is acceptable to break storage compatibility and public API compatibility if that keeps the implementation clean. Do not add compatibility shims.

## Roles

### Orchestrator

The Orchestrator owns the loop. It decides the next smallest useful unit of work, spawns workers, spawns reviewers, integrates results, and decides whether the milestone is complete.

Responsibilities:

- Maintain a short backlog of tasks.
- Keep every task inside Milestone 0 scope.
- Prefer narrow workers with clear inputs and outputs.
- Ensure the first coding task writes a failing end-to-end test under `vector/tests`.
- After each worker completes, spawn at least one reviewer agent for that worker's output.
- Use reviewer feedback to spawn fix workers or make scoped edits.
- Run tests and formatting at appropriate checkpoints.
- Exit only when the Definition of Done is satisfied.

### Worker Agents

Worker agents do coding tasks. Each worker receives:

- A precise objective.
- Relevant files to inspect.
- Scope boundaries.
- Expected verification commands.

Worker rules:

- Read the local code before editing.
- Keep changes scoped to the task.
- Prefer existing project patterns.
- Do not implement non-goals.
- Add or update tests for behavior touched by the task.
- Report changed files, important decisions, and verification results.

Suggested workers:

- `TestWorker`: creates the first failing `vector/tests` BM25 end-to-end test.
- `SchemaApiWorker`: adds `FieldType::Text`, text metadata validation, and BM25 query API types.
- `SerdeStorageWorker`: adds FTS keys, values, record types, storage helpers, and merge behavior.
- `IndexerWorker`: adds `TextAttributeSummary` and `FtsIndexDelta`, then wires FTS indexing into the Indexer under `vector/src/write/indexer/tree/`.
- `QueryWorker`: implements tokenizer reuse and BM25 query execution in the query engine.
- `FixWorker`: addresses test failures and reviewer findings.

### Reviewer Agents

Reviewer agents review worker output. A reviewer must not rewrite the code directly unless the Orchestrator explicitly assigns it as a fix task.

Reviewer responsibilities:

- Look for correctness bugs, compile failures, missing tests, scope drift, and poor fit with existing patterns.
- Check that later milestones have not been implemented.
- Check storage encoding/decoding round trips.
- Check query scoring order and result loading behavior.
- Check that existing ANN search and metadata filter tests are not broken.
- Produce findings with file/line references when possible.

## Loop

Run this loop until Milestone 0 is complete:

1. Orchestrator inspects current state and picks the next task.
2. Orchestrator spawns one or more Worker agents for independent tasks.
3. Workers implement their assigned changes and run focused verification where possible.
4. Orchestrator spawns Reviewer agents to review each worker's output.
5. Orchestrator triages reviewer findings.
6. Orchestrator spawns FixWorkers or performs small scoped edits.
7. Orchestrator runs the relevant tests.
8. Repeat until the Definition of Done is satisfied.

Do not exit because the code "looks close". The loop exits only after verification passes or after a real blocker is documented.

## First Task: End-to-End Test

The first Worker must add an end-to-end test under `vector/tests`, preferably `vector/tests/text_search.rs`.

The test should:

- Create a `VectorDb` with `StorageConfig::InMemory`.
- Define at least one metadata field with `FieldType::Text`, for example `body`.
- Write several vectors whose text field values have overlapping and non-overlapping terms.
- Flush the database.
- Search with a BM25 query against the text field.
- Assert that documents containing the query terms are returned.
- Assert ranking demonstrates BM25 behavior, for example a document with repeated query terms outranks a document with only one occurrence, and documents without query terms are absent.
- Assert result field selection still returns the stored text field unless a projection says otherwise.

Use simple, deterministic test text. Avoid stemming-sensitive expectations. Example query terms can be lowercase ASCII words such as `quick`, `fox`, `database`, and `vector`.

This test should be created before implementation work proceeds. It is expected to fail initially.

## Implementation Guidance

### Public API and Model

Add a text field type:

- Extend `FieldType` with `Text = 4`.
- A `Text` metadata field is written as an `AttributeValue::String`.
- `Text` fields are always indexed for FTS. The existing `indexed` flag should not be required for FTS indexing.
- Validation should reject non-string values for `Text` fields.

Add a scoring API based on the RFC:

- Add `Bm25Query { field: String, query: String }`.
- Add `ScoreBy::{Ann(Vec<f32>), Bm25(Bm25Query)}`.
- Change `Query` to hold `score_by`.
- Keep ergonomic constructors, for example `Query::ann(vector)`, `Query::bm25(field, query)`, and preserve `Query::new(vector)` as an ANN convenience if that minimizes unrelated churn.
- Preserve `with_limit`, `with_filter`, and `with_fields` behavior.

HTTP/protobuf request support is not required for Milestone 0 unless existing tests force small adjustments. The end-to-end test can use the Rust API directly.

### Tokenization

Add a small tokenizer abstraction/module so write and query paths use the same tokenization.

Required behavior for Milestone 0:

- Deterministic.
- Lowercase normalized for ASCII input.
- Splits simple prose into word terms.
- Ignores empty terms.

The RFC mentions ICU4X. Use it if it fits cleanly, adding dependencies at the workspace/vector level. If that becomes a blocker, implement a small local tokenizer for Milestone 0 and isolate it so ICU4X can replace it later.

### Write Preparation

Extend `VectorWrite` with text summaries:

```rust
pub(crate) struct TextAttributeSummary {
    pub(crate) terms: BTreeMap<String, usize>,
    pub(crate) length: usize,
}
```

Use `HashMap<String, TextAttributeSummary>` on `VectorWrite`.

`VectorDb::prepare_vector_write` should:

- Validate schema and dimensions as it does today.
- For each `FieldType::Text` attribute, require `AttributeValue::String`.
- Tokenize the string.
- Build term frequencies and total field length.
- Store the summary on the `VectorWrite`.

### Indexing

FTS indexing must be done by the Indexer under `vector/src/write/indexer/tree/`.

Add a new `FtsIndexDelta` similar in spirit to `SearchIndexDelta`, and add it to `VectorIndexDelta`.

For each inserted vector, `Indexer`/`WriteVectors` should use the `VectorWrite.text_attribute_summaries` to update `delta.fts_index`:

- Add a posting entry for every `(field, term)` with vector id, term frequency, and the document's length norm as described in the RFC.
- Update term statistics for every `(field, term)` once per document.
- Update field statistics for each text field, including document count and total length.
- Optionally write vector field stats if useful for future work, but do not build delete handling around it in this milestone.

For updates/deletes:

- Do not implement FTS deletion bitmap handling.
- Do not implement compaction cleanup.
- If stale FTS postings are encountered during BM25 search, it is acceptable for the query path to skip candidates whose forward index lookup no longer returns vector data.
- Do not add tests that require correct FTS upsert/delete semantics in Milestone 0.

### Storage Formats

Keep the current key prefix shape for Milestone 0:

```text
<subsystem (1 byte)><version (1 byte)><record_tag (1 byte)><suffix>
```

Do not add a segment byte.

Add FTS record types and key structs under `vector/src/serde/` / `vector/src/serde/key.rs` using the RFC-assigned tags. Do not invent alternate FTS tags.

Use these RFC tags and discriminators:

- `TermPostings`: record tag `0x0d`, key by field and term, trailing secondary discriminator `0x00`.
- `TermStats`: record tag `0x0d`, key by field and term, trailing secondary discriminator `0x01`.
- `VectorFieldStats`: record tag `0x0e`.
- `FieldStats`: record tag `0x0f`.
- `Deletions`: record tag `0x0c` is out of scope for Milestone 0.

Use the storage format from RFC-0006, with only the segment byte omitted from keys for this milestone:

- `TermPostingsValue` stores RFC posting blocks, not a simplified flat format.
- Posting blocks should use the RFC `Postings` and `Skip` block structure.
- Posting blocks should store document ids in descending vector id order.
- `docs` should use the RFC bitpacked representation.
- `freqs` should use the RFC delta-encoded representation.
- `norms` should use the RFC fixed byte array representation.
- Emit a skip block for every postings block as described by the RFC.
- Skip blocks should include the RFC traversal data needed for this milestone, such as `last_id` and `length`.
- Do not encode impacts in skip blocks for Milestone 0. If the concrete Rust type keeps an `impacts` field to match the RFC shape, encode it as empty and do not use it.
- `TermStatsValue`, `VectorFieldStatsValue`, and `FieldStatsValue` should follow the RFC schemas.
- Merges may concatenate term postings and sum stats as described in the RFC.

Add unit tests for key and value encode/decode round trips and merge behavior.

### Query Path

Branch query execution on `query.score_by`.

For `ScoreBy::Ann`, preserve current ANN behavior.

For `ScoreBy::Bm25`:

- Tokenize the query string using the shared tokenizer.
- Load postings for all query terms in the requested field.
- Load term stats and field stats.
- Score candidates with simple BM25.
- Use constants `k1 = 1.2` and `b = 0.75` unless the implementation has a better existing home for defaults.
- Use the IDF formula from the RFC:

```text
idf(t) = ln(((N - n(t) + 0.5) / (n(t) + 0.5)) + 1)
```

- Use `N = field_stats.count`.
- Use `avgdl = field_stats.total_length / field_stats.count`.
- Accumulate scores over query terms.
- Return the top `query.limit` results in descending BM25 score.
- Resolve vector data through the forward index storage path and skip candidates with missing vector data.
- Apply `include_fields` projection to BM25 results.

Metadata filters for BM25 are not required for this milestone unless they are easy to reuse. If implemented, apply filters before final scoring or before final result selection and do not make BM25 corpus stats filter-aware.

## Verification

Run focused checks as the loop progresses:

```bash
cargo test -p opendata-vector --test text_search
cargo test -p opendata-vector
cargo fmt
```

If clippy is practical within the session, run:

```bash
cargo clippy -p opendata-vector --all-targets --all-features -- -D warnings
```

At minimum, the final loop must run:

```bash
cargo fmt
cargo test -p opendata-vector --test text_search
```

If broader tests cannot run, document why and what remains unverified.

## Definition of Done

Milestone 0 is done only when all of these are true:

- `vector/tests/text_search.rs` or equivalent exists and passes.
- Text fields can be declared in vector metadata.
- Writes tokenize text fields and index term postings/statistics through the Indexer.
- `FtsIndexDelta` exists and is wired into `VectorIndexDelta`.
- BM25 queries use FTS postings/statistics and return correctly ranked documents.
- ANN queries still work through the existing path.
- No segmentation, delete bitmap, compaction filter, custom compaction, or MaxScore work was added.
- Rust formatting has been run.
- The Orchestrator has addressed reviewer findings or documented why a finding is intentionally deferred.

When reporting completion, include:

- Summary of code changes.
- Tests run and results.
- Any intentionally deferred RFC items.
