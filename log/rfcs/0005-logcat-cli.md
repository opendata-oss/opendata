# RFC 0005: logcat CLI

**Status**: Draft

**Authors**:
- Jason Gustafson <12502538+hachikuji@users.noreply.github.com>

## Summary

This RFC proposes `logcat`, a minimal command-line tool for reading entries from an OpenData-Log. Similar to `kafkacat` for Kafka, it provides a simple way to consume log entries from the command line for debugging, monitoring, and scripting.

## Motivation

OpenData-Log lacks a lightweight CLI tool for quick inspection and tailing of log data. Developers need to inspect entries during development, tail a key in real-time, and script log consumption in shell pipelines. A simple CLI fills this gap without requiring HTTP server setup.

## Goals

- Read entries from a log by key
- Support tailing/following with configurable poll interval
- Support resuming from a specific sequence number

## Non-Goals

- Writing/appending to the log
- Complex filtering or transformations (pipe to `jq`, `grep`, etc.)
- Multiple keys in a single invocation

## Design

### CLI Interface

```
logcat --key <KEY> [OPTIONS]

Options:
  -k, --key <KEY>           Key to read from (required)
  -c, --config <PATH>       Path to config file (TOML)
  -s, --start-seq <SEQ>     Starting sequence number [default: 0]
  -f, --follow              Follow mode: poll for new entries
  -i, --interval <MS>       Poll interval in milliseconds [default: 1000]
  -o, --output <FORMAT>     Output format: csv or json [default: csv]
```

### Configuration File

The config file uses TOML format matching `log::Config` (storage and segmentation settings). If not provided, uses the default `StorageConfig`.

### Output Format

**CSV** (default): Each entry is a single line with comma-separated fields:

```
<sequence>,<value>
```

**JSON**: Each entry is a JSON object per line (NDJSON):

```json
{"sequence":0,"value":"hello world"}
```

Values are printed as UTF-8 if valid, otherwise base64-encoded (with a `"encoding":"base64"` field in JSON mode).

### Behavior

**Default mode**: Read all entries for the key from `start_seq` to the end, then exit.

**Follow mode** (`-f`): After reading existing entries, poll for new entries at the specified interval until interrupted.

### Implementation

A new binary in the `log` crate, gated behind a `cli` feature flag:

```toml
[[bin]]
name = "logcat"
path = "src/bin/logcat.rs"
required-features = ["cli"]
```

Uses `LogDbReader` with `scan()` to iterate entries.

### Usage Examples

```bash
# Read all entries for key "events" (CSV output)
logcat -k events

# Output as JSON
logcat -k events -o json

# Follow a key in real-time
logcat -k events -f

# Resume from sequence 100 with custom poll interval
logcat -k events -s 100 -f -i 100
```

## Alternatives

**HTTP API with curl**: Requires running the server and base64 encoding. `logcat` provides direct, simpler access.

## Open Questions

- Should the tool be named `logcat` or `logdbcat` to avoid confusion with Android's logcat?

## Updates

| Date       | Description |
|------------|-------------|
| 2026-02-04 | Initial draft |
