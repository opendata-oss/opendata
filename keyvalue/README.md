# opendata-keyvalue

Key-value database built on [SlateDB](https://github.com/slatedb/slatedb) for
[OpenData](https://github.com/opendata-oss/opendata).

## Quickstart

### 1. Run a focused smoke test

```bash
cargo test -p opendata-keyvalue should_put_and_get_single_key
```

### 2. Minimal usage example

```rust
use bytes::Bytes;
use common::StorageConfig;
use keyvalue::{Config, KeyValueDb, KeyValueRead};

let kv = KeyValueDb::open(Config {
    storage: StorageConfig::InMemory,
})
.await?;

kv.put(Bytes::from("user:1"), Bytes::from("alice")).await?;
let value = kv.get(Bytes::from("user:1")).await?;
assert_eq!(value, Some(Bytes::from("alice")));
```

## License

MIT
