#!/usr/bin/env bash
#
# Smoke test for the vector quickstart.
#
# Runs docker compose, waits for the embedded reader to complete,
# then verifies HTTP search works against both writer and reader servers.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

COMPOSE="docker compose"
TIMEOUT=300  # max seconds to wait for embedded-reader to finish
PASSED=0
FAILED=0

cleanup() {
    echo ""
    echo "--- Cleaning up ---"
    $COMPOSE down -v --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

fail() {
    echo "FAIL: $1"
    FAILED=$((FAILED + 1))
}

pass() {
    echo "PASS: $1"
    PASSED=$((PASSED + 1))
}

assert_contains() {
    local label="$1" body="$2" expected="$3"
    if echo "$body" | grep -q "$expected"; then
        pass "$label"
    else
        fail "$label (expected '$expected' in response)"
        echo "  Response: $body"
    fi
}

# ---------------------------------------------------------------
# 1. Start everything and wait for embedded-reader to complete
# ---------------------------------------------------------------
echo "=== Starting quickstart ==="
$COMPOSE up --build -d

echo "=== Waiting for embedded-reader to finish (timeout: ${TIMEOUT}s) ==="
elapsed=0
while [ $elapsed -lt $TIMEOUT ]; do
    status=$($COMPOSE ps embedded-reader --format '{{.State}}' 2>/dev/null || echo "unknown")
    if [ "$status" = "exited" ]; then
        exit_code=$($COMPOSE ps embedded-reader --format '{{.ExitCode}}' 2>/dev/null || echo "1")
        if [ "$exit_code" = "0" ]; then
            pass "embedded-reader exited successfully"
        else
            fail "embedded-reader exited with code $exit_code"
            echo "--- embedded-reader logs ---"
            $COMPOSE logs embedded-reader
            exit 1
        fi
        break
    fi
    sleep 5
    elapsed=$((elapsed + 5))
    if [ $((elapsed % 30)) -eq 0 ]; then
        echo "  ...still waiting (${elapsed}s)"
    fi
done

if [ $elapsed -ge $TIMEOUT ]; then
    fail "Timed out waiting for embedded-reader"
    echo "--- docker compose logs ---"
    $COMPOSE logs --tail=50
    exit 1
fi

# Verify the expected output appeared in the logs
logs=$($COMPOSE logs embedded-reader 2>&1)
assert_contains "embedded-reader printed completion message" "$logs" "Embedded reader complete"

# ---------------------------------------------------------------
# 2. Embed a query via the embedding server
# ---------------------------------------------------------------
echo ""
echo "=== Testing HTTP endpoints ==="

EMBEDDING=$(curl -sf -X POST http://localhost:9000/embed \
  -H "Content-Type: application/json" \
  -d '{"texts": ["How does ownership work?"]}' \
  | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin)['embeddings'][0]))")

if [ -z "$EMBEDDING" ]; then
    fail "Could not get embedding from embedding server"
    exit 1
fi
pass "embedding server returned an embedding"

# ---------------------------------------------------------------
# 3. Search via the writer (port 8080)
# ---------------------------------------------------------------
writer_response=$(curl -sf -X POST http://localhost:8080/api/v1/vector/search \
  -H "Content-Type: application/protobuf+json" \
  -H "Accept: application/protobuf+json" \
  -d "{\"vector\": $EMBEDDING, \"k\": 3, \"includeFields\": [\"book\", \"section\"]}")

assert_contains "writer search returned results" "$writer_response" '"results"'
assert_contains "writer search returned a score" "$writer_response" '"score"'
assert_contains "writer search returned book field" "$writer_response" '"book"'

# Verify projection: vector embedding should NOT be in the response
if echo "$writer_response" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for r in data.get('results', []):
    if 'vector' in r.get('vector', {}).get('attributes', {}):
        sys.exit(1)
" 2>/dev/null; then
    pass "writer search: projection excluded vector embedding"
else
    fail "writer search: projection did not exclude vector embedding"
fi

# ---------------------------------------------------------------
# 4. Search via the reader (port 8081)
# ---------------------------------------------------------------
reader_response=$(curl -sf -X POST http://localhost:8081/api/v1/vector/search \
  -H "Content-Type: application/protobuf+json" \
  -H "Accept: application/protobuf+json" \
  -d "{\"vector\": $EMBEDDING, \"k\": 3, \"includeFields\": [\"book\", \"section\"]}")

assert_contains "reader search returned results" "$reader_response" '"results"'
assert_contains "reader search returned a score" "$reader_response" '"score"'
assert_contains "reader search returned book field" "$reader_response" '"book"'

# Verify projection on reader too
if echo "$reader_response" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for r in data.get('results', []):
    if 'vector' in r.get('vector', {}).get('attributes', {}):
        sys.exit(1)
" 2>/dev/null; then
    pass "reader search: projection excluded vector embedding"
else
    fail "reader search: projection did not exclude vector embedding"
fi

# ---------------------------------------------------------------
# 5. Search with a filter
# ---------------------------------------------------------------
filtered_response=$(curl -sf -X POST http://localhost:8080/api/v1/vector/search \
  -H "Content-Type: application/protobuf+json" \
  -H "Accept: application/protobuf+json" \
  -d "{\"vector\": $EMBEDDING, \"k\": 3, \"includeFields\": [\"book\", \"section\"], \"filter\": {\"eq\": {\"field\": \"book\", \"value\": \"The Rust Programming Language\"}}}")

assert_contains "filtered search returned results" "$filtered_response" '"results"'

# All results should be from the Rust book
if echo "$filtered_response" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for r in data.get('results', []):
    book = r.get('vector', {}).get('attributes', {}).get('book', '')
    if book != 'The Rust Programming Language':
        sys.exit(1)
" 2>/dev/null; then
    pass "filtered search: all results from Rust book"
else
    fail "filtered search: unexpected book in results"
fi

# ---------------------------------------------------------------
# Summary
# ---------------------------------------------------------------
echo ""
echo "========================================"
echo "  Results: $PASSED passed, $FAILED failed"
echo "========================================"

if [ $FAILED -gt 0 ]; then
    exit 1
fi
