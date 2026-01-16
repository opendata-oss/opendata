#!/usr/bin/env python3
"""
Mock metrics server that exposes Prometheus-format metrics for testing.

Usage:
    python mock_metrics_server.py [--port PORT]

The server exposes metrics at /metrics endpoint.
"""

import argparse
import random
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread

# Simulated metric values
start_time = time.time()
request_count = 0
error_count = 0


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global request_count, error_count

        if self.path == "/metrics":
            request_count += 1
            # Randomly increment error count
            if random.random() < 0.1:
                error_count += 1

            metrics = self._generate_metrics()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.end_headers()
            self.wfile.write(metrics.encode("utf-8"))
        elif self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_response(404)
            self.end_headers()

    def _generate_metrics(self):
        uptime = time.time() - start_time

        # Generate some realistic-looking metrics
        metrics = []

        # Counter: total requests
        metrics.append("# HELP mock_requests_total Total number of requests received")
        metrics.append("# TYPE mock_requests_total counter")
        metrics.append(f"mock_requests_total {request_count}")

        # Counter: errors
        metrics.append("# HELP mock_errors_total Total number of errors")
        metrics.append("# TYPE mock_errors_total counter")
        metrics.append(f"mock_errors_total {error_count}")

        # Gauge: uptime
        metrics.append("# HELP mock_uptime_seconds Time since server started")
        metrics.append("# TYPE mock_uptime_seconds gauge")
        metrics.append(f"mock_uptime_seconds {uptime:.2f}")

        # Gauge: random CPU usage simulation
        metrics.append("# HELP mock_cpu_usage_percent Simulated CPU usage")
        metrics.append("# TYPE mock_cpu_usage_percent gauge")
        metrics.append(f"mock_cpu_usage_percent {random.uniform(10, 90):.2f}")

        # Gauge: random memory usage simulation
        metrics.append("# HELP mock_memory_usage_bytes Simulated memory usage")
        metrics.append("# TYPE mock_memory_usage_bytes gauge")
        metrics.append(f"mock_memory_usage_bytes {random.randint(100000000, 500000000)}")

        # Histogram: simulated request duration
        metrics.append("# HELP mock_request_duration_seconds Request duration histogram")
        metrics.append("# TYPE mock_request_duration_seconds histogram")
        # Simulate bucket counts
        le_buckets = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        cumulative = 0
        for le in le_buckets:
            cumulative += random.randint(0, 10)
            metrics.append(f'mock_request_duration_seconds_bucket{{le="{le}"}} {cumulative}')
        metrics.append(f'mock_request_duration_seconds_bucket{{le="+Inf"}} {cumulative + random.randint(0, 5)}')
        metrics.append(f"mock_request_duration_seconds_sum {random.uniform(0.1, 10.0):.4f}")
        metrics.append(f"mock_request_duration_seconds_count {cumulative}")

        # Metrics with labels
        metrics.append("# HELP mock_http_requests_by_status HTTP requests by status code")
        metrics.append("# TYPE mock_http_requests_by_status counter")
        metrics.append(f'mock_http_requests_by_status{{status="200"}} {request_count - error_count}')
        metrics.append(f'mock_http_requests_by_status{{status="500"}} {error_count}')

        # Multi-label metric
        metrics.append("# HELP mock_api_calls API calls by endpoint and method")
        metrics.append("# TYPE mock_api_calls counter")
        metrics.append(f'mock_api_calls{{endpoint="/users",method="GET"}} {random.randint(100, 1000)}')
        metrics.append(f'mock_api_calls{{endpoint="/users",method="POST"}} {random.randint(10, 100)}')
        metrics.append(f'mock_api_calls{{endpoint="/orders",method="GET"}} {random.randint(50, 500)}')
        metrics.append(f'mock_api_calls{{endpoint="/orders",method="POST"}} {random.randint(20, 200)}')

        return "\n".join(metrics) + "\n"

    def log_message(self, format, *args):
        # Suppress default logging, or customize it
        print(f"[{self.log_date_time_string()}] {args[0]}")


def main():
    parser = argparse.ArgumentParser(description="Mock Prometheus metrics server")
    parser.add_argument("--port", type=int, default=8080, help="Port to listen on (default: 8080)")
    args = parser.parse_args()

    server = HTTPServer(("0.0.0.0", args.port), MetricsHandler)
    print(f"Mock metrics server running on http://0.0.0.0:{args.port}")
    print(f"Metrics available at http://0.0.0.0:{args.port}/metrics")
    print("Press Ctrl+C to stop")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()