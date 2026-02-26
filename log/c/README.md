# opendata-log-c

C FFI bindings for [opendata-log](../). Produces a shared library (`libopendata_log_c.dylib` / `.so` / `.dll`) and a static library (`libopendata_log_c.a`) with a generated C header, so any language with C FFI support (Java/Panama, Go, Python, etc.) can use opendata-log.

## Building

Requires a [Rust toolchain](https://rustup.rs/).

```sh
cargo build -p opendata-log-c --release
```

Outputs:
- `target/release/libopendata_log_c.{dylib,so,dll}` — shared library
- `target/release/libopendata_log_c.a` — static library
- `log/c/include/opendata_log.h` — generated C header

## Linking

Link against the shared or static library and include the generated header:

```sh
# Shared
cc -o example example.c -L target/release -lopendata_log_c -I log/c/include

# Static (macOS — adjust system libs per platform)
cc -o example example.c target/release/libopendata_log_c.a -I log/c/include \
    -framework CoreFoundation -framework Security -lm -lpthread
```

## Usage

Every function returns an `opendata_log_result_t`. Check `result.kind == OPENDATA_LOG_OK` for success. On error, `result.message` contains a Rust-allocated string — free it with `opendata_log_result_free()`.

Opaque handles (`opendata_log_t`, `opendata_log_iterator_t`, etc.) are always heap-allocated and must be closed with their corresponding `_close()` function.

Byte buffers returned through out-pointers (`out_key`, `out_value`) are Rust-allocated and must be freed with `opendata_log_bytes_free()`.

### Example

```c
#include "opendata_log.h"
#include <stdio.h>
#include <string.h>

int main(void) {
    opendata_log_result_t r;

    // Open an in-memory log
    opendata_log_config_t config = {
        .storage_type = OPENDATA_LOG_STORAGE_IN_MEMORY,
        .seal_interval_ms = -1,  // use default
    };
    opendata_log_t *log = NULL;
    r = opendata_log_open(&config, &log);
    if (r.kind != OPENDATA_LOG_OK) {
        fprintf(stderr, "open failed: %s\n", r.message);
        opendata_log_result_free(r);
        return 1;
    }

    // Append two records to the "events" key
    const char *key = "events";
    const uint8_t *keys[]   = {(const uint8_t *)key, (const uint8_t *)key};
    size_t key_lens[]        = {strlen(key), strlen(key)};
    const char *v0 = "hello", *v1 = "world";
    const uint8_t *values[] = {(const uint8_t *)v0, (const uint8_t *)v1};
    size_t value_lens[]      = {strlen(v0), strlen(v1)};
    uint64_t start_seq = 0;

    r = opendata_log_try_append(log, keys, key_lens, values, value_lens, 2, &start_seq);
    if (r.kind != OPENDATA_LOG_OK) {
        fprintf(stderr, "append failed: %s\n", r.message);
        opendata_log_result_free(r);
        opendata_log_close(log);
        return 1;
    }
    printf("appended at sequence %llu\n", (unsigned long long)start_seq);

    // Flush to storage
    r = opendata_log_flush(log);
    if (r.kind != OPENDATA_LOG_OK) {
        opendata_log_result_free(r);
        opendata_log_close(log);
        return 1;
    }

    // Scan all entries for the key
    opendata_log_seq_range_t range = {
        .start = {OPENDATA_LOG_BOUND_UNBOUNDED, 0},
        .end   = {OPENDATA_LOG_BOUND_UNBOUNDED, 0},
    };
    opendata_log_iterator_t *iter = NULL;
    r = opendata_log_scan(log, (const uint8_t *)key, strlen(key), &range, &iter);
    if (r.kind != OPENDATA_LOG_OK) {
        opendata_log_result_free(r);
        opendata_log_close(log);
        return 1;
    }

    bool present;
    uint8_t *out_key, *out_value;
    size_t out_key_len, out_value_len;
    uint64_t sequence;

    while (1) {
        r = opendata_log_iterator_next(iter, &present,
            &out_key, &out_key_len, &sequence, &out_value, &out_value_len);
        if (r.kind != OPENDATA_LOG_OK) {
            opendata_log_result_free(r);
            break;
        }
        if (!present) break;

        printf("seq=%llu key=%.*s value=%.*s\n",
            (unsigned long long)sequence,
            (int)out_key_len, out_key,
            (int)out_value_len, out_value);

        opendata_log_bytes_free(out_key, out_key_len);
        opendata_log_bytes_free(out_value, out_value_len);
    }

    opendata_log_iterator_close(iter);
    opendata_log_close(log);
    return 0;
}
```

### Storage backends

Use the object store constructors to configure SlateDB-backed persistent storage:

```c
// Local filesystem
opendata_log_object_store_t *store = NULL;
opendata_log_object_store_local("/tmp/my-data", &store);

opendata_log_config_t config = {
    .storage_type = OPENDATA_LOG_STORAGE_SLATEDB,
    .slatedb_path = "my-log",
    .object_store = store,
    .seal_interval_ms = 3600000,  // 1 hour
};

opendata_log_t *log = NULL;
opendata_log_open(&config, &log);

// ... use log ...

opendata_log_close(log);
opendata_log_object_store_close(store);
```

AWS S3:
```c
opendata_log_object_store_t *store = NULL;
opendata_log_object_store_aws("us-west-2", "my-bucket", &store);
```

## API reference

See [`include/opendata_log.h`](include/opendata_log.h) for the full API. The functions are grouped as:

| Group | Functions |
|-------|-----------|
| Lifecycle | `opendata_log_open`, `_flush`, `_close` |
| Write | `opendata_log_try_append`, `_append_timeout` |
| Read | `opendata_log_scan`, `_count`, `_count_with_options`, `_list_keys`, `_list_segments` |
| Reader | `opendata_log_reader_open`, `_close`, `_scan`, `_count`, `_count_with_options`, `_list_keys`, `_list_segments` |
| Iterators | `opendata_log_iterator_next`, `_close`, `opendata_log_key_iterator_next`, `_close` |
| Object stores | `opendata_log_object_store_in_memory`, `_local`, `_aws`, `_close` |
| Memory | `opendata_log_result_free`, `_bytes_free`, `_segments_free` |
