"""
In-memory metrics collector for performance monitoring.
Tracks request counts, latencies, and component-specific metrics.
"""

import time
from typing import Dict, Any
from collections import defaultdict


class MetricsCollector:
    """Collects and exposes runtime metrics for a node."""

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.start_time = time.time()

        # General metrics
        self.request_count = 0
        self.error_count = 0
        self.latencies: list = []

        # Component-specific metrics
        self.lock_metrics = {
            "acquired": 0,
            "released": 0,
            "denied": 0,
            "deadlocks_detected": 0,
            "active_locks": 0,
        }

        self.queue_metrics = {
            "messages_pushed": 0,
            "messages_consumed": 0,
            "messages_acked": 0,
            "messages_redelivered": 0,
        }

        self.cache_metrics = {
            "hits": 0,
            "misses": 0,
            "invalidations": 0,
            "evictions": 0,
            "entries": 0,
        }

        self.raft_metrics = {
            "term": 0,
            "role": "follower",
            "leader_id": None,
            "elections_started": 0,
            "votes_granted": 0,
        }

    @property
    def uptime(self) -> float:
        """Return uptime in seconds."""
        return time.time() - self.start_time

    def record_request(self, latency_ms: float = 0):
        """Record an API request."""
        self.request_count += 1
        if latency_ms > 0:
            self.latencies.append(latency_ms)
            # Keep only last 1000 latencies to avoid memory growth
            if len(self.latencies) > 1000:
                self.latencies = self.latencies[-1000:]

    def record_error(self):
        """Record an error."""
        self.error_count += 1

    def get_latency_stats(self) -> Dict[str, float]:
        """Calculate latency statistics."""
        if not self.latencies:
            return {"avg_ms": 0, "min_ms": 0, "max_ms": 0, "p99_ms": 0}

        sorted_lat = sorted(self.latencies)
        p99_idx = int(len(sorted_lat) * 0.99)

        return {
            "avg_ms": round(sum(sorted_lat) / len(sorted_lat), 2),
            "min_ms": round(sorted_lat[0], 2),
            "max_ms": round(sorted_lat[-1], 2),
            "p99_ms": round(sorted_lat[p99_idx], 2),
        }

    def get_all_metrics(self) -> Dict[str, Any]:
        """Return all metrics as a dictionary."""
        return {
            "node_id": self.node_id,
            "uptime_seconds": round(self.uptime, 2),
            "total_requests": self.request_count,
            "total_errors": self.error_count,
            "latency": self.get_latency_stats(),
            "lock": self.lock_metrics.copy(),
            "queue": self.queue_metrics.copy(),
            "cache": self.cache_metrics.copy(),
            "raft": self.raft_metrics.copy(),
        }


# Global metrics instance (initialized per node)
_metrics: MetricsCollector = None


def get_metrics() -> MetricsCollector:
    """Get the global metrics collector."""
    global _metrics
    if _metrics is None:
        _metrics = MetricsCollector("unknown")
    return _metrics


def init_metrics(node_id: str) -> MetricsCollector:
    """Initialize the global metrics collector with a node ID."""
    global _metrics
    _metrics = MetricsCollector(node_id)
    return _metrics
