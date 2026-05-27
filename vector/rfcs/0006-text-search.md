# RFC-0006: Full Text Search

**Status**: Draft

**Authors**:
- [Rohan Desai](https://github.com/rodesai)

## Summary

This RFC details the design for supporting full text search (FTS) in OpenData Vector.

## Motivation

Modern search use cases typically require a combination of semantic search and full text search.
Semantic search can easily miss documents if the user's query is interested in specific keywords 
(proper nouns, emails, etc). To mitigate this, applications can combine semantic and full text 
search results to improve overall search quality.

## Overview

Like ANN, FTS returns the top K documents that are closest to some query. Search requires 
computing a set of candidate documents, assigning the documents some relevance score, and then 
returning the documents with the highest score. Unlike ANN, FTS scores documents based on the 
presence of query terms rather than distance to an embedding.

### BM25

These days the de-facto ranking function for scoring documents is BM25. BM25 scores documents 
using a "bag of words" model. It factors in only the frequency of each query term in a document 
(along with some statistics about each term and the corpus), so it's disjunctive and ignores 
position. More specifically, it combines the inverse-document frequency (IDF) for each term with 
the frequency of each term normalized by the document length and saturated using static 
parameters. IDF represents how infrequent a given term is. It increases as the number of 
documents with a given term decreases.

For a query `q` and document `D`, BM25 is commonly written as:

$$
\mathrm{score}(D, q) =
\sum_{t \in q}
\mathrm{IDF}(t)
\cdot
\frac{
f(t, D)(k_1 + 1)
}{
f(t, D) + k_1 \left(1 - b + b \cdot \frac{|D|}{\mathrm{avgdl}}\right)
}
$$

Where:

- \(t\): a query term
- \(f(t, D)\): frequency of term \(t\) in document \(D\)
- \(|D|\): length of document \(D\)
- \(avgdl\): average document length in the corpus
- \(k_1\): term-frequency saturation parameter, often around `1.2` to `2.0`
- \(b\): document-length normalization parameter, often around `0.75`

A common IDF variant is:

$$
\mathrm{IDF}(t) =
\log \left(
\frac{N - n(t) + 0.5}{n(t) + 0.5} + 1
\right)
$$

Where:

- \(N\): total number of documents in the corpus
- \(n(t)\): number of documents containing term \(t\)

To efficiently score documents, search databases do the following:

First, when ingesting documents, each document is tokenized to break it up into discrete terms. 

The database stores per-term posting lists. Each posting list stores the set of documents that 
contain the term, along with the term's frequency in the document (`f(t, D)`) and (often a 
quantized representation of) the document's size (`|D|`) in number of tokens. The database also 
stores statistics about each term and the corpus. In particular, for each term it stores the 
number of documents containing the term (`n(t)`) and for the corpus the total number of 
documents (`N`) and total document length (used to compute `avgdl`).

At query time, the database tokenizes the query, loads the posting lists for each term, and 
scores the contained documents using the BM25 formula. It then returns the top documents.

### MaxScore

Scoring every document in the union of the posting lists can be very expensive. Common terms 
could contain a huge number of documents. To optimize this, search dbs use a family of 
algorithms that keep track of the max possible score contribution of each term, and avoid
generating candidates from the postings of terms that are not required to be present for a 
document to enter the top K results. One subfamily of these algorithms, which we'll use for 
Vector's FTS is called MaxScore.

The first versions of MaxScore relied on 2 properties of BM25 to compute global max scores for 
each term without reading postings at all:
- The IDF component depends solely on per-term and corpus stats.
- The document-specific component saturates as term-frequency within a document increases.

MaxScore tracks the minimum score required to enter the top K and groups terms into disjoint 
essential and non-essential sets, where the total max score of the terms in the non-essential 
set is less than the current minimum score. For a document to enter the top K it must contain at 
least one term from the essential set, so only documents in the postings from the essential set 
need be considered. As more documents are scored, the minimum score rises and the terms in the 
essential set shrinks. By default, MaxScore computes the sets by sorting the terms by max score 
contribution and greedily selecting the lowest scoring terms. This is good as the terms with the 
lowest max score typically have the longest postings lists (because they have a low IDF component).
This approach also minimizes the number of essential terms whose documents must be scored.

Modern iterations of MaxScore try to bound max scores further by tracking max scores for ranges 
within a posting list, aligning the iterators to the ranges, and then using the localized max 
scores instead. This gives tighter bounds on max score and allows for much more skipping. This 
family of algorithms is called BlockMaxScore

Here's a rough sketch of the algorithm. Assume that each posting supports in iterator that 
observes a window of postings. The iterators return the next document at the start of the window,
and return max scores over the full window:

```rust
trait PostingIterator {
    /// Advances the end of the iterator's window to a document greater than or equal to the 
    /// specified ID
    fn advance_end(document_id: u64);
    
    /// Return the next document ID, its term frequency, and its length
    fn next() -> (u64, u32, u32);
    
    /// Return the max score over the current window using local summary statistics about term 
    /// frequencies and document length
    fn max_score() -> f32;
}
```

Then, the scoring/ranking algorithm looks very roughly like:
1. Let `TOPK` be the current `TOPK` documents. Let `Min(TOPK)` be the minimum score in `TOPK`. 
   Initialize `TOPK` to empty.
2. Let `window_start` be the document id of the lowest document in all posting lists.
3. Let `I_0`, `I_1`, .. `I_n` be the set of posting iterators for terms in the query.
3. Loop until `window_start` >= `max_document_id` where `max_document_id` is the highest doc id
   1. Decide on some window end `window_end`. Selection of the `window_end` is heuristic to 
      balance per-loop overhead (when the window is too small) with looser bounds (when the 
      window is too large).
   2. For each iterator `I` in `I_0`, `I_1`, .. `I_n`
      1. `I.advance_end(window_end)`
      2. Compute `I`'s max score contribution for the window using `I.max_score()` and `Min(TOPK)`
   3. Compute which iterators are essential/non-essential for the window.
   4. Score all documents for the essential iterators.
   5. Update `TOPK` with the scored documents.

### Field Norms

We'll store field norms rather than document lengths in postings. This reduces posting size and
overhead for tracking block-level scoring statistics. The norm is the field length quantized to
1-byte. We'll use the same quantization formula as lucene (
https://github.com/apache/lucene/blob/main/lucene/core/src/java/org/apache/lucene/util/SmallFloat.java#L147).
The approach there loses precision for longer documents, which is fine because the length
normalization contribution to BM25 also becomes less significant as document size increases.

### Problems

Efficient FTS using the approach described above requires solving a few problems:

#### Efficient Posting Iteration w/ MAXSCORE

Posting iterators need to be able to efficiently advance to specific documents, and in the case 
of BlockMaxScore compute max scores for the window. Let's address these two problems separately.

Internally, each posting list stores blocks of postings of a fixed size (e.g. 256 docs). Storing 
postings in blocks helps iterators seek cheaply if you can identify the block's document range and
the position of the next block. We can store that in a different type of block immediately preceding
the postings block. Let's call this a Skip block. It stores the last doc id and length of the
subsequent block. It is typical (e.g. in Lucene) to further improve on this by also storing Skip 
blocks for larger ranges (e.g. every 32 blocks in Lucene).

**Impacts**

To support cheaply computing max scores for a window, you can store max score statistics for the 
documents covered by the Skip block. In particular, you can store the "dominating" term 
frequencies (F), length (L) pairs, called Impacts, of the contained documents. Impacts have a 
partial order. A document with higher F _and_ lower L than another document is guaranteed to 
have a greater score. Otherwise, their ordering depends on the specific BM25 parameters. Dominating
Impacts are those where no other FL pair is strictly greater. Databases typically quantize doc 
lengths to 1 byte (see
https://github.com/apache/lucene/blob/main/lucene/core/src/java/org/apache/lucene/util/SmallFloat.java#L147
), so at most 256 pairs need to be stored. In practice, this is typically much fewer as only a few 
documents dominate.

#### Dealing with Large Posting Lists

Posting lists for common terms can be extremely large, possibly containing nearly every document in 
the corpus in the extreme. Assuming ~8 bytes per document, a db with 100 million documents would 
have a posting of ~800MB for a term like "the". In practice such terms may be treated as 
stop words by the tokenizer and ignored. Still, there will likely be a few common terms that 
reference a significant percentage of the vectors. SlateDB allows us to update these incrementally 
using merges, but the entire posting still needs to be loaded into memory at query time.

For now, we'll accept the extra overhead during query processing. The _Future Work_ section 
discusses how we can address this by chunking posting lists.

#### Deletion Handling

When vectors are deleted, we need to remove the vector from term postings and update term 
statistics for the vector's terms. Our current approach to handling deletes is to read some 
metadata about the vector (`VectorIndexMetadata`) that gives us enough information to apply 
updates to the attribute postings. Then, Vector marks the vector deleted for each of its 
attributes and centroid postings. This approach poses 2 problems for FTS. First, we depend on 
this metadata being relatively small so that its likely to be in cache and can be cheaply read 
even if it spills to disk. Per-term stats and postings blow up the size of this metadata as 
Vector would need to track every term for each indexed field in each vector. Second, we'd need 
to track deletions for every term in a document. We expect documents to have 100s of terms, so 
this adds write amplification and some space amplification.

For FTS, Vector will write deleted vectors to a bitmap stored in SlateDB (referred to as the 
Deletions Bitmap). A custom compaction filter applies the deletes from the sorted runs 
participating in the compaction to the postings and term stats deltas present in those runs. The 
bitmap entries are filtered out when compacting SR 0. The bitmap and the FTS postings and term 
stats will go to a dedicated segment for FTS so that we can aggressively run full compactions 
when lots of deletes accumulate.

To account for deletes when processing queries, Vector will hold an in-memory copy of the bitmap.

There are a few things we have to account for with this approach:
1. We need some way to clear the deletions bitmap entries from storage
2. Application of deletions to term stats is async. If too many deletes accumulate it can 
   degrade scoring quality. Note that BM25 is forgiving - document frequencies being off by 
   10-20% won't meaningfully change scores.
3. We need to keep the size of the in-memory bitmap in check. This means we need to apply the 
   bitmap updates to storage often enough, and then apply those storage updates to the in-memory 
   copy.
4. A large bitmap will blow up cold query latencies as it must be loaded from storage before 
   queries can run.
5. We need to preserve snapshot-aligned views of the bitmap for queries

To address 1, Vector will filter out Deletions merge entries when a compaction targets the last 
SR (SR 0) in the segment. Writes to the bitmap are always sequenced after the writes that added 
the vector to the underlying postings/stats, and compactions always merge consecutive SRs.
This means that once a Deletions merge entry reaches SR0, the documents it references will have 
already been removed from the postings/stats.

To address 2, 3, and 4 we'll customize compaction of the FTS segment by tracking the current number 
of outstanding deletes. When this count is greater than some % of total vectors, Vector will do 
a full compaction of the segment. This requires a custom compaction policy that wraps and defaults 
to the `SizeTieredCompactionScheduler` from slatedb, but adds this special case for the FTS 
segment. We'll default the threshold to 20%. Let's gut-check the write overhead and resulting 
memory usage from the bitmap. Let's assume a stable (not changing logical size) db with 100M 
documents with ~200 terms each (~1KB). The bitmap will need to hold up to 20M entries before its 
fully compacted. If we assume 2 bytes per entry (very conservative), that's ~38MB, which is very 
reasonable. Total postings size for the 100M documents, assuming ~4 bytes/entry is 100M * 200 
terms * 4 bytes/term = 74GB. At a write rate of 1K upserts/second, 20% deletes accumulate every 
~5.5 hours. Looking at it another way, this adds a 5x factor (each upsert is written once and 
then written 4 more times in SR0 before it's compacted away) to the write amplification factor 
for the FTS data, so it's roughly doubled. This is not ideal, but feels manageable.

To address 3 and 5 we'll simply load the bitmap from storage after every flush and put it in the 
`LastDbSnapshot` published by the Flusher.

## Configurations

- `delete_compaction_threshold`: The threshold, expressed as a percentage of total vectors, for 
  the number of deletes before which a major compaction is triggered.
- `target_posting_block_size`: The target posting list block size.

## API

### Metadata Field Spec

We will extend the `MetadataFieldSpec` enum to add a new variant called `Text`. The Collection
metadata can optionally support an arbitrary number of `Text` fields. The `indexed` property
for `Text` fields is ignored, and `Text` fields are always indexed.

```rust,ignore
pub enum FieldType {
    String = 0,
    Int64 = 1,
    Float64 = 2,
    Bool = 3,
    Text = 4,
    /// Vector type (tag 255) - used internally to store vectors as a special field.
    /// Not valid in collection metadata schemas.
    Vector = 255,
}
```

### Writes

Writes specify text fields by adding an attribute with the field name and an `AttributeValue` of
type `String`:

```rust,ignore
let vector = Vector::builder("vector-id", vec![0.1, 0.2, 0.3])
    .attribute(
        "text-field",
        AttributeValue::String("the quick fox jumps over the lazy dog".to_string()))
    .build()
```

When a document is written, Vector tokenizes all fields of type `Text`
_before_ writing them to the Write Coordinator. For now, we will tokenize using the
[icu4x crate](https://github.com/unicode-org/icu4x), which does basic word segmentation.

### Reads

To support fts queries, we'll change the query API to support arbitrary ranking strategies instead
of hard-coding vector search:

```rust,ignore
pub struct Bm25Query {
    pub field: String,
    pub query: String
}

pub enum ScoreBy {
    Ann(Vec<f32>),
    Bm25(Bm25Query)
}

#[derive(Debug, Clone)]
pub struct Query {
    // Method to use for scoring documents
    pub score_by: ScoreBy,
    /// Maximum number of results to return (default: 10).
    pub limit: usize,
    /// Optional metadata filter.
    pub filter: Option<Filter>,
    /// Which fields to include in results (default: All).
    pub include_fields: FieldSelection,
}

impl Query {
    pub fn ann(vector: Vec<f32>) -> Self {
        Self {
            score_by: ScoreBy::Ann(vector),
            limit: 10,
            filter: None,
            include_fields: FieldSelection::All,
        }
    }
    
    pub fn bm25(field: String, query: String) -> Self {
        Self {
            score_by: ScoreBy::Bm25(Bm25Query{ field, query }),
            limit: 10,
            filter: None,
            include_fields: FieldSelection::All,
        }
    }

    ...
}
```

We intentionally leave more complex queries out of scope for this iteration. In particular, we
omit the ability to customize the specific BM25 scoring formula by:
    - Combining multiple bm25 fields in 1 query
    - Specifying boosts for terms or attributes
    - Combining bm25 scoring with other inputs like attribute values

In the future, we can extend the `ScoreBy` enum to support these more complex expressions.

### Hybrid Search

We will not support hybrid search (combining ANN with BM25) within a single query. ANN and BM25
scores are not comparable, so typically this requires separate passes to find the top-K documents,
and then another pass to combine top-K lists and re-rank. The specific algorithm used to fuse and
re-rank is use-case specific, so this is left to the client of Vector.

## Storage

To support efficient full-text-search, we'll store the following indexing data in SlateDB:
- Deletions: A bitmap of all internal vector IDs that have been deleted (either via a delete or 
  update)
- Term Postings: Posting lists for each query term present in an indexed field.
- Term Statistics: Global statistics about a query term. Used to factor into scoring.
- Field Statistics: Global statistics about an indexed field. Used for scoring.

#### Key Layout/Segmentation

To support segmentation we will add a segment prefix to each key, so the keys become
```
<subsystem (1 byte)><segment id (1 byte)><version (1 byte)><record_tag (1 byte)><suffix>
```

We'll segment Vector into the following segments:
| ID     | Name               | Description                             |
|--------|--------------------|-----------------------------------------|
| `0x00` | Default            | Singletons, forward, and inverted index |
| `0x01` | ANN                | ANN Postings                            |
| `0x02` | FTS                | FTS postings and stats                  |

| ID     | Name               | Segment |
|--------|--------------------|---------|
| `0x01` | `CollectionMeta`   | Default | 
| `0x02` | `PostingList`      | ANN     | 
| `0x03` | `IdDictionary`     | Default | 
| `0x04` | `VectorData`       | Default | 
| `0x05` | `MetadataIndex`    | Default | 
| `0x06` | `SeqBlock`         | Default | 
| `0x07` | `CentroidStats`    | ANN     | 
| `0x08` | `Centroids`        | ANN     | 
| `0x09` | `CentroidInfo`     | ANN     |
| `0x0a` | `CentroidSeqBlock` | ANN     | 
| `0x0b` | `VectorIndexData`  | Default |
| `0x0c` | `Deletions`        | FTS     |
| `0x0d` | `TermPostings`     | FTS     |
| `0x0d` | `TermStats`        | FTS     |
| `0x0e` | `VectorStats`      | FTS     |
| `0x0f` | `FieldStats`       | FTS     |

#### `Deletions` (`RecordType::Deletions` = `0x0c`)

**Key Layout**

```
<subsystem (1 byte)><segment (1 byte)><version (1 byte)><record_tag (1 byte)>
```

**Value Schema**

```
┌────────────────────────────────────────────────────────────────┐
│                     DeletionsValue                             │
├────────────────────────────────────────────────────────────────┤
│  deletions: RoaringBitmap                                      | 
└────────────────────────────────────────────────────────────────┘
```

**Structure**

`DeletionsValue` is simply a bitmap of deleted vector IDs. The value is always written as a 
`Merge`. Merges union the contained bitmaps and serialize the result.

### `Term Postings` (`RecordType::TermPostings (0x0d`)

**Key Layout**

```
<subsystem (1 byte)><segment (1 byte)><version (1 byte)><record tag (1 byte)><field len (2 bytes)><field (n bytes)><term len (2 bytes)><term (n bytes)>0x00
```

The key is prefixed by the field and then the term, followed by a secondary type discriminator 
(`0x00`). The inclusion of a secondary type prefix allows us to colocate the term statistics 
deltas with the postings. This way the compaction filter can easily update term statistics to 
reflect deletes after compacting the relevant postings.

**Value Schema**

```
┌────────────────────────────────────────────────────────────────┐
│                     TermPostingsValue                          │
├────────────────────────────────────────────────────────────────┤
│  blocks: FixedElementArray<TermPostingsBlock>                  │
│                                                                │
│  TermPostingsBlock                                             │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  type:       0x00 (Postings) | 0x01 (Skip)               │  │
|  |  len:        u32                                         |  |
│  │  block:      Postings | Skip                             |  | 
│  └──────────────────────────────────────────────────────────┘  │
│  Postings                                                      │
│  ┌──────────────────────────────────────────────────────────┐  │
|  |  docs:       Bitpacked                                   |  |
│  │  freqs:      Delta-Encoded                               │  │
|  |  norms:      FixedElementArray<byte>                     |  |
│  └──────────────────────────────────────────────────────────┘  │
│  Skip                                                          │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  last_id:    u64                                         │  │
|  |  impacts:    Array<Impact>                               |  |
|  |  length:     u64                                         |  |
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

** Structure **

The postings stores a sequence of postings for the term in order of descending vector id 
(highest first), alongside block-level scoring stats and skip data for efficient traversal.
Each postings value is a sequence of typed blocks with the type discriminated using a 
discriminator field (`type`). The type is either `Postings` or `Skip`. Blocks appear in 
reverse-document ID order. So each `Posting` block holds ids strictly lower than the preceding 
`Posting` block. We'll target 256 postings per block.

`Postings` stores:
- `docs`: Document ids contained in the block. We'll encode these using the bitpacking crate 
  (https://github.com/quickwit-oss/bitpacking)
- `freqs`: delta-encoded frequencies for each document.
- `norms`: The size norm of each document.

`Skips` stores skip data that can be used to efficiently find postings for a document in the list.
It also contains max score stats for the skipped range.
- `last_id`: The id of the last document in the range (subsequent block).
- `impacts`: An array of (freq, norm) pairs where freq is the frequency of the term in a doc and
  norm is the size norm of the associated document. These represent the set of dominating size
  norms where either the freq is larger or the norm is lower than another vector in the range.
  So, for example for the set `[(10, 5), (5, 15), (11, 7), (12, 3), (13, 8), (11, 1)]` the
  dominating impacts are `[(13, 8), (12, 3), (11, 1)]`. This should be bounded to 256 entries since
  norms are 1 byte. In practice this will be much lower, especially for small ranges
- `length`: The length of the skipped data 

For the initial version of this feature we will only encode a skip for every block. In later 
iterations, we will additionally encode skips for longer ranges to support more efficient 
traversal of the posting list.

The postings are written as a SlateDB `Merge`. Merges are simple buffer concatenations.

### Term Statistics 

**Key Layout**

```
<subsystem (1 byte)><segment (1 byte)><version (1 byte)><record tag (1 byte)><field len (2 bytes)><field (n bytes)><term len (2 bytes)><term (n bytes)>0x01
```

**Value Schema**

```
┌────────────────────────────────────────────────────────────────┐
│                     TermStatsValue                             │
├────────────────────────────────────────────────────────────────┤
│  freq: i64                                                     |
└────────────────────────────────────────────────────────────────┘
```

**Structure**

`freq`: A delta to the number of vectors that contain the term. This is tracked as a delta so 
that the compaction filter doesn't depend on a posting and its stats update being present in the 
same SR (though that should always be the case).

### Vector Field Statistics

**Key Layout**

```
<subsystem (1 byte)><segment (1 byte)><version (1 byte)><record tag (1 byte)><vector id (8 bytes)>
```

**Value Schema**

```
┌────────────────────────────────────────────────────────────────┐
│                     VectorFieldStatsValue                      │
├────────────────────────────────────────────────────────────────┤
│  length: FixedElementArray(string, u32)>                       |
└────────────────────────────────────────────────────────────────┘
```

**Structure**

`length`: The document's length in number of terms for each text field. 

### Field Statistics

**Key Layout**

```
<subsystem (1 byte)><segment (1 byte)><version (1 byte)><record tag (1 byte)><field len (2 bytes)><field (n bytes)>
```

**Value Schema**

```
┌────────────────────────────────────────────────────────────────┐
│                     FieldStatsValue                            │
├────────────────────────────────────────────────────────────────┤
│  freq: i64                                                     |
|  total_length: i64                                             |
|  deletes: i64                                                  |
└────────────────────────────────────────────────────────────────┘
```

**Structure**

`freq`: A delta to the number of vectors that contain the term. This is tracked as a delta so
that the compaction filter doesn't depend on a posting and its stats update being present in the
same SR (though that should always be the case).
`total_length`: A delta to the total length of the field (in terms) across all vectors.
`deletes`: A delta to the number of deleted vectors (used to apply the major compaction 
threshold heuristic).

## Index Maintenance

The indexer accepts Vector upserts and deletes that include `Text` fields. `VectorDb#write` 
tokenizes and computes term-level counts before publishing to the Write Coordinator, and 
includes this information in `VectorWrite`:

```rust
/// Summarizes an attribute of type `Text`
pub(crate) struct TextAttributeSummary {
    /// Maps the terms present in a field to the frequency (number of occurrences)
    terms: BTreeMap<String, usize>,
    /// The length of the field in number of terms
    length: usize,
}


/// A vector write ready for the coordinator.
///
/// The write path validates and enqueues this struct.
/// The delta handles ID allocation, dictionary lookup, centroid assignment, and updates.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VectorWrite {
    /// User-provided external ID.
    pub(crate) external_id: String,
    /// Vector embedding values.
    pub(crate) values: Vec<f32>,
    /// All attributes including the vector field.
    pub(crate) attributes: Vec<(String, AttributeValue)>,
    /// Summaries for all attributes of type `Text`
    pub(crate) text_attribute_summaries: HashMap<String, TextAttributeSummary>
}
```

For every batch of writes, the Indexer builds up a delta containing the corresponding mutations 
to storage. This delta specifies the updates to the fields from the **Storage** section above.

For each write, the Indexer:
1. Add an entry for the vector for each of its terms in each of its `Text` fields. The entry 
   specifies the vector ID, the frequency, and the document's length norm. It can collect these 
   into simple `Vec<(VectorId, u32, u8)>`, and then when flushing pack these into a `Merge` 
   whose value contains the encoded posting blocks. The posting will store the documents in 
   descending ID order.
2. Update `TermStatsValue` for every term in each of its `Text` attribute.
3. Put the `VectorFieldStatsValue` with lengths for each `Text` attribute.
4. Update the `FieldStatsValue` for every `Text` attribute.

For each delete (including upserts which implicitly delete), the Indexer:
1. Adds the deleted vector id to a `Merge` to the deletions bitmap.

Finally, the indexer tracks the number of deletes and total number of vectors in-memory and exposes 
this to the compaction policy (TODO: the protocol here needs to be fleshed out - e.g. how will 
the indexer be notified when the major compaction ends. The policy would need to tell it the 
sources its compacting and then the indexer/flusher can watch the manifest and then reload the 
deletion bitmap after it's done)

### Compaction Policy

Vector will use a custom compaction policy `VectorCompactionPolicy` that wraps the 
`SizeTieredCompactionPolicy` in SlateDB. `VectorCompactionPolicy` watches the total number of 
deletes and vectors and triggers a major compaction of the FTS segment whenever the % of 
deletes crosses `delete_compaction_threshold`. Otherwise it delegates to 
`SizeTieredCompactionPolicy`.

### Compaction Filter

Actual cleanup of postings is done by a compaction filter. If the compaction doesn't target the 
FTS segment, it does nothing, Otherwise, it:
- Loads the deletions bitmap into memory from the `Merge` entries present in a compaction.
- Filters out the deletion bitmap entries if compacting to SR 0.
- Compacts posting lists by removing deleted vectors and re-aligning blocks to 
  `target_posting_block_size`. As it does this it tracks the delta to the number of vectors with 
  the posting's term
- Applies the delta to the term statistics.
- Removes `VectorFieldStatistics` entries for deleted vectors and accumulates field stats deltas.
- Applies the delta to the field stats. Note that the current compaction filter implementation
  in slatedb does not allow inserting new keys, so this behaviour depends on the `FieldStats`
  being present for each field in each SR. For now, we'll guarantee this by emitting a FieldStats
  delta on each flush. In the future we can add support for adding keys during compaction.

## Query Processing

When processing FTS queries we'll follow the high-level procedure from the **BM25** section:
1. Load postings for query terms
2. Iterate over postings (optimized w/ BlockMaxScore) and score documents
3. Accumulate the TOPK

A few things to call out here:

**Interaction with Filters**

Queries can specify attribute filters in addition to a BM25 query. Filters are applied before 
scoring, but the BM25 score itself is not filter-aware - meaning we don't factor in the filters 
when resolving the corpus size and document frequencies.

**Vectorization**

There are many points during iterating/scoring that can be accelerated with SIMD:
- decoding the bitpacked vector IDs (quickwit's crate already does this)
- decoding frequency deltas
- bm25 socring itself

We'll punt this for the first milestones and tackle it as we start benchmarking.

## Milestones

**0: Insert-Only / Prune Deletes by consulting Forward Index**
- Add formats for tracking postings and statistics
- Update indexer to emit postings and statistics updates
- Implement simple BM25 scoring on the query path (no maxscore, no SIMD, etc)

**1: Support Deletes**
- Add formats for the deletions bitmap
- Add the custom compaction filter

**2: Minimize Delete Overhead**
- Add the segmenter to move FTS data to a segment
- Custom compaction policy to trigger major compaction when over `delete_compaction_threshold`

**3: Benchmark/Optimize Query Path**
- Compute/add impacts to Skip blocks
- Implement BlockMaxScore
- Vectorize scoring for essential posting lists

## Future Work

### Chunked Postings

As discussed above, posting lists as proposed in this RFC can grow quite large, which blows up 
the memory required to process a query. To mitigate this we could store postings in chunks where 
each chunk is identified by the highest document id in the chunk. The indexer would _always_ 
write posting lists as PUTs, and then the compaction filter could compact adjacent posting lists 
together until they reach some target size. This requires the ability emit new keys from 
compaction filters. We can revisit based on the overhead we actually see.

### Multiple FTS Segments

As a way to mitigate heavy major compactions to handle deletes, we could maintain the FTS index 
in multiple segments, where each segment stores data for a range of document IDs. Then, each 
segment's major compaction becomes smaller. The problem with this approach is that we'll 
eventually wind up with tiny segments where most documents were deleted, which hurts query 
performance. So we'll need some background process that merges multiple such segments together.

## Alternatives Considered

### Global Bitmap Without FTS Segmentation

Earlier versions of this RFC proposed maintaining a global bitmap without placing the FTS index 
in its own segment. This approach makes it impractical to pull our main lever for dealing with 
delete overhead - running a major compaction.

### Maintain Term Stats/Posting Deletes During Indexing

Refers to tracking terms in `VectorIndexData` and updating stats/postings for deletes when 
indexing. Discussion is inline.

# References
- https://en.wikipedia.org/wiki/Okapi_BM25
- https://github.com/apache/lucene
- https://turbopuffer.com/blog/fts-v2-postings
- https://research.engineering.nyu.edu/~suel/papers/bmm.pdf
