"""Unit test untuk MetricsCollector."""

import pytest
from src.utils.metrics import MetricsCollector


class TestMetricsCollector:
    def test_request_count_increments(self):
        m = MetricsCollector("test")
        m.record_request(5.0)
        m.record_request(10.0)
        assert m.request_count == 2

    def test_latency_avg(self):
        m = MetricsCollector("test")
        m.record_request(4.0)
        m.record_request(6.0)
        stats = m.get_latency_stats()
        assert stats["avg_ms"] == 5.0

    def test_all_metrics_keys(self):
        m = MetricsCollector("test")
        result = m.get_all_metrics()
        assert "lock" in result
        assert "queue" in result
        assert "cache" in result
        assert "raft" in result

    def test_cache_hit_recording(self):
        m = MetricsCollector("test")
        m.cache_metrics["hits"] += 5
        m.cache_metrics["misses"] += 1
        all_m = m.get_all_metrics()
        assert all_m["cache"]["hits"] == 5

    def test_lock_metrics_recording(self):
        m = MetricsCollector("test")
        m.lock_metrics["acquired"] += 3
        m.lock_metrics["released"] += 2
        all_m = m.get_all_metrics()
        assert all_m["lock"]["acquired"] == 3
        assert all_m["lock"]["released"] == 2
