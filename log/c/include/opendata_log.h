#ifndef OPENDATA_LOG_H
#define OPENDATA_LOG_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#define OPENDATA_LOG_STORAGE_IN_MEMORY 0

#define OPENDATA_LOG_STORAGE_SLATEDB 1

#define OPENDATA_LOG_READ_VISIBILITY_MEMORY 0

#define OPENDATA_LOG_READ_VISIBILITY_REMOTE 1

#define OPENDATA_LOG_BOUND_UNBOUNDED 0

#define OPENDATA_LOG_BOUND_INCLUDED 1

#define OPENDATA_LOG_BOUND_EXCLUDED 2

typedef enum opendata_log_error_kind_t {
  OPENDATA_LOG_OK = 0,
  OPENDATA_LOG_ERROR_STORAGE,
  OPENDATA_LOG_ERROR_ENCODING,
  OPENDATA_LOG_ERROR_INVALID_INPUT,
  OPENDATA_LOG_ERROR_INTERNAL,
  OPENDATA_LOG_ERROR_QUEUE_FULL,
  OPENDATA_LOG_ERROR_TIMEOUT,
  OPENDATA_LOG_ERROR_SHUTDOWN,
  OPENDATA_LOG_ERROR_INVALID_RECORD,
} opendata_log_error_kind_t;

typedef struct opendata_log_iterator_t opendata_log_iterator_t;

typedef struct opendata_log_key_iterator_t opendata_log_key_iterator_t;

typedef struct opendata_log_object_store_t opendata_log_object_store_t;

typedef struct opendata_log_reader_t opendata_log_reader_t;

/**
 * Opaque handle representing a registered durable-sequence subscription.
 *
 * Created by `opendata_log_subscribe_durable` and destroyed by
 * `opendata_log_unsubscribe_durable`. Must be freed explicitly even if the
 * log has been closed; closing the log aborts the task but does not free
 * this handle.
 */
typedef struct opendata_log_subscription_t opendata_log_subscription_t;

typedef struct opendata_log_t opendata_log_t;

typedef struct opendata_log_result_t {
  enum opendata_log_error_kind_t kind;
  char *message;
} opendata_log_result_t;

typedef struct opendata_log_config_t {
  uint8_t storage_type;
  const char *slatedb_path;
  const struct opendata_log_object_store_t *object_store;
  const char *settings_path;
  int64_t seal_interval_ms;
  /**
   * Controls which data is visible to reads.
   * Use `OPENDATA_LOG_READ_VISIBILITY_MEMORY` (default) to include in-memory data,
   * or `OPENDATA_LOG_READ_VISIBILITY_REMOTE` to only see data confirmed durable.
   */
  uint8_t read_visibility;
} opendata_log_config_t;

typedef struct opendata_log_seq_bound_t {
  uint8_t kind;
  uint64_t value;
} opendata_log_seq_bound_t;

typedef struct opendata_log_seq_range_t {
  struct opendata_log_seq_bound_t start;
  struct opendata_log_seq_bound_t end;
} opendata_log_seq_range_t;

typedef struct opendata_log_segment_bound_t {
  uint8_t kind;
  uint32_t value;
} opendata_log_segment_bound_t;

typedef struct opendata_log_segment_range_t {
  struct opendata_log_segment_bound_t start;
  struct opendata_log_segment_bound_t end;
} opendata_log_segment_range_t;

typedef struct opendata_log_segment_t {
  uint32_t id;
  uint64_t start_seq;
  int64_t start_time_ms;
} opendata_log_segment_t;

typedef struct opendata_log_reader_config_t {
  uint8_t storage_type;
  const char *slatedb_path;
  const struct opendata_log_object_store_t *object_store;
  const char *settings_path;
  int64_t refresh_interval_ms;
} opendata_log_reader_config_t;

/**
 * Callback invoked when the durable-sequence watermark advances.
 *
 * The first invocation carries the current value at subscription time; each
 * subsequent invocation carries the latest observed value (intermediate
 * values may be coalesced).
 *
 * The callback is invoked on a tokio worker thread, not the caller's thread.
 * It must not call other `opendata_log_*` functions on the same log handle
 * (this would deadlock the runtime).
 */
typedef void (*opendata_log_durable_callback_t)(uint64_t durable_sequence, void *user_data);

struct opendata_log_result_t opendata_log_open(const struct opendata_log_config_t *config,
                                               struct opendata_log_t **out_log);

struct opendata_log_result_t opendata_log_flush(struct opendata_log_t *log);

struct opendata_log_result_t opendata_log_close(struct opendata_log_t *log);

struct opendata_log_result_t opendata_log_try_append(struct opendata_log_t *log,
                                                     const uint8_t *const *keys,
                                                     const uintptr_t *key_lens,
                                                     const uint8_t *const *values,
                                                     const uintptr_t *value_lens,
                                                     uintptr_t record_count,
                                                     uint64_t *out_start_sequence);

struct opendata_log_result_t opendata_log_append_timeout(struct opendata_log_t *log,
                                                         const uint8_t *const *keys,
                                                         const uintptr_t *key_lens,
                                                         const uint8_t *const *values,
                                                         const uintptr_t *value_lens,
                                                         uintptr_t record_count,
                                                         uint64_t timeout_ms,
                                                         uint64_t *out_start_sequence);

struct opendata_log_result_t opendata_log_scan(const struct opendata_log_t *log,
                                               const uint8_t *key,
                                               uintptr_t key_len,
                                               const struct opendata_log_seq_range_t *seq_range,
                                               struct opendata_log_iterator_t **out_iterator);

struct opendata_log_result_t opendata_log_count(const struct opendata_log_t *log,
                                                const uint8_t *key,
                                                uintptr_t key_len,
                                                const struct opendata_log_seq_range_t *seq_range,
                                                uint64_t *out_count);

struct opendata_log_result_t opendata_log_list_keys(const struct opendata_log_t *log,
                                                    const struct opendata_log_segment_range_t *segment_range,
                                                    struct opendata_log_key_iterator_t **out_iterator);

struct opendata_log_result_t opendata_log_list_segments(const struct opendata_log_t *log,
                                                        const struct opendata_log_seq_range_t *seq_range,
                                                        struct opendata_log_segment_t **out_segments,
                                                        uintptr_t *out_count);

/**
 * Reads the current durable-sequence watermark.
 *
 * `*out_sequence` is set to N such that all records with `sequence < N` are
 * durably persisted. Initial value is 0; advances as the underlying storage
 * confirms durability (no explicit flush required, though `opendata_log_flush`
 * does force it).
 */
struct opendata_log_result_t opendata_log_durable_sequence(const struct opendata_log_t *log,
                                                           uint64_t *out_sequence);

/**
 * Subscribes to durable-sequence watermark changes.
 *
 * `callback` is invoked once on registration with the current watermark, then
 * on each subsequent advancement. The callback runs on a tokio worker thread
 * (not the caller's thread) and **must not call other `opendata_log_*`
 * functions on the same log handle** — doing so will deadlock the runtime.
 *
 * Intermediate values may be coalesced; the callback always receives the
 * latest observed value.
 *
 * `user_data` is passed back verbatim to each callback invocation. The
 * caller is responsible for ensuring it remains valid (and safe for
 * concurrent access from worker threads) for the entire lifetime of the
 * subscription, **including any in-flight callback that may still be
 * running after `opendata_log_unsubscribe_durable` returns** (see that
 * function's contract).
 */
struct opendata_log_result_t opendata_log_subscribe_durable(const struct opendata_log_t *log,
                                                            void (*callback)(uint64_t durable_sequence,
                                                                             void *user_data),
                                                            void *user_data,
                                                            struct opendata_log_subscription_t **out_subscription);

/**
 * Cancels a durable-sequence subscription and frees the handle.
 *
 * **Best-effort with respect to in-flight callbacks.** This call signals the
 * spawned task to stop and returns immediately. If a callback is currently
 * executing on a worker thread, it will run to completion *after* this
 * function returns. A new callback for this subscription will not be
 * scheduled.
 *
 * **Caller obligation:** `user_data` must remain valid until every
 * in-flight invocation of the callback has finished. The C API does not
 * synchronize with worker threads on unsubscribe; freeing `user_data`
 * immediately after this call returns is a use-after-free if a callback is
 * mid-execution. A safe pattern is to keep `user_data` alive as long as
 * any subscription that references it, e.g. by tying it to the lifetime of
 * the `opendata_log_t` (close the log first, then free `user_data` once no
 * pending callback work could possibly remain).
 *
 * Safe to call after `opendata_log_close` (the spawned task will already
 * have exited because its watch channel closed when the log dropped).
 */
struct opendata_log_result_t opendata_log_unsubscribe_durable(struct opendata_log_subscription_t *subscription);

struct opendata_log_result_t opendata_log_iterator_next(struct opendata_log_iterator_t *iterator,
                                                        bool *out_present,
                                                        uint8_t **out_key,
                                                        uintptr_t *out_key_len,
                                                        uint64_t *out_sequence,
                                                        uint8_t **out_value,
                                                        uintptr_t *out_value_len);

struct opendata_log_result_t opendata_log_iterator_close(struct opendata_log_iterator_t *iterator);

struct opendata_log_result_t opendata_log_key_iterator_next(struct opendata_log_key_iterator_t *iterator,
                                                            bool *out_present,
                                                            uint8_t **out_key,
                                                            uintptr_t *out_key_len);

struct opendata_log_result_t opendata_log_key_iterator_close(struct opendata_log_key_iterator_t *iterator);

/**
 * Installs a global `tracing` subscriber that writes to stderr.
 *
 * `filter` is an `EnvFilter` directive string (same syntax as `RUST_LOG`),
 * e.g. `"info"`, `"slatedb=debug"`, or `"slatedb=debug,opendata_log=info"`.
 * Pass `NULL` to fall back to the `RUST_LOG` environment variable, or
 * `"info"` if `RUST_LOG` is unset.
 *
 * Must be called at most once per process, before `opendata_log_open`. The
 * underlying `tracing` global default can only be set once; if a subscriber
 * is already installed (e.g. by another Rust component embedded in the same
 * host), this returns `OPENDATA_LOG_ERROR_INTERNAL`.
 *
 * Hosts that already manage their own `tracing` subscriber should not call
 * this function.
 */
struct opendata_log_result_t opendata_log_enable_logging(const char *filter);

void opendata_log_result_free(struct opendata_log_result_t result);

void opendata_log_bytes_free(uint8_t *data, uintptr_t len);

void opendata_log_segments_free(struct opendata_log_segment_t *segments, uintptr_t count);

struct opendata_log_result_t opendata_log_object_store_in_memory(struct opendata_log_object_store_t **out_store);

struct opendata_log_result_t opendata_log_object_store_local(const char *path,
                                                             struct opendata_log_object_store_t **out_store);

struct opendata_log_result_t opendata_log_object_store_aws(const char *region,
                                                           const char *bucket,
                                                           struct opendata_log_object_store_t **out_store);

struct opendata_log_result_t opendata_log_object_store_close(struct opendata_log_object_store_t *store);

struct opendata_log_result_t opendata_log_reader_open(const struct opendata_log_reader_config_t *config,
                                                      struct opendata_log_reader_t **out_reader);

struct opendata_log_result_t opendata_log_reader_close(struct opendata_log_reader_t *reader);

struct opendata_log_result_t opendata_log_reader_scan(const struct opendata_log_reader_t *reader,
                                                      const uint8_t *key,
                                                      uintptr_t key_len,
                                                      const struct opendata_log_seq_range_t *seq_range,
                                                      struct opendata_log_iterator_t **out_iterator);

struct opendata_log_result_t opendata_log_reader_count(const struct opendata_log_reader_t *reader,
                                                       const uint8_t *key,
                                                       uintptr_t key_len,
                                                       const struct opendata_log_seq_range_t *seq_range,
                                                       uint64_t *out_count);

struct opendata_log_result_t opendata_log_reader_list_keys(const struct opendata_log_reader_t *reader,
                                                           const struct opendata_log_segment_range_t *segment_range,
                                                           struct opendata_log_key_iterator_t **out_iterator);

struct opendata_log_result_t opendata_log_reader_list_segments(const struct opendata_log_reader_t *reader,
                                                               const struct opendata_log_seq_range_t *seq_range,
                                                               struct opendata_log_segment_t **out_segments,
                                                               uintptr_t *out_count);

/**
 * Installs a global `metrics-rs` Prometheus recorder that captures SlateDB
 * metrics emitted from any `LogDb` or `LogDbReader` opened in this process.
 *
 * **Idempotent and lock-free.** Safe to call any number of times from any
 * thread; only the first call actually installs the recorder, subsequent
 * calls observe the cached result. Safe to call before or after
 * `opendata_log_open` / `opendata_log_reader_open`.
 *
 * Returns `OPENDATA_LOG_ERROR_INTERNAL` if a different `metrics-rs` recorder
 * is already installed in the process (e.g. by another embedded Rust
 * component). In that case `opendata_log_render_metrics` will return an
 * empty buffer.
 */
struct opendata_log_result_t opendata_log_init_telemetry(void);

/**
 * Renders the current SlateDB metrics in Prometheus text-exposition format.
 *
 * On success, allocates a heap buffer and writes its address and length to
 * `*out_data` / `*out_len`. The caller owns the buffer and must release it
 * with `opendata_log_bytes_free`.
 *
 * Lazily initializes the recorder on first call if `opendata_log_init_telemetry`
 * was not invoked explicitly. If a foreign `metrics-rs` recorder occupies
 * the global slot, returns success with `*out_data = NULL` and `*out_len = 0`
 * (an empty buffer is not an error).
 *
 * `out_data` and `out_len` must be non-null. The function never blocks.
 */
struct opendata_log_result_t opendata_log_render_metrics(uint8_t **out_data, uintptr_t *out_len);

#endif  /* OPENDATA_LOG_H */
