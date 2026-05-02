"""Integration tests for the distributed synchronization system."""

import pytest
import asyncio
from src.nodes.lock_manager import DistributedLockManager, LockType
from src.nodes.queue_node import ConsistentHashRing, DistributedQueue, Message
from src.nodes.cache_node import MESICache, CacheState
from src.utils.metrics import MetricsCollector


class TestLockManager:
    def setup_method(self):
        self.metrics = MetricsCollector("test")
        self.lm = DistributedLockManager("test_node", metrics=self.metrics)

    @pytest.mark.asyncio
    async def test_acquire_exclusive_lock(self):
        result = await self.lm.acquire_lock("res1", "client1", "exclusive", ttl=10)
        assert result["status"] == "granted"

    @pytest.mark.asyncio
    async def test_shared_locks_compatible(self):
        r1 = await self.lm.acquire_lock("res1", "c1", "shared")
        r2 = await self.lm.acquire_lock("res1", "c2", "shared")
        assert r1["status"] == "granted"
        assert r2["status"] == "granted"

    @pytest.mark.asyncio
    async def test_exclusive_blocks_shared(self):
        await self.lm.acquire_lock("res1", "c1", "exclusive", ttl=10)
        r2 = await self.lm.acquire_lock("res1", "c2", "shared", timeout=0.5)
        assert r2["status"] == "timeout"

    @pytest.mark.asyncio
    async def test_release_lock(self):
        await self.lm.acquire_lock("res1", "c1", "exclusive")
        result = await self.lm.release_lock("res1", "c1")
        assert result["status"] == "released"

    @pytest.mark.asyncio
    async def test_lock_status(self):
        await self.lm.acquire_lock("res1", "c1", "exclusive")
        status = self.lm.get_status()
        assert status["active_count"] == 1

    def test_deadlock_detection(self):
        self.lm.wait_for_graph["c1"].add("c2")
        self.lm.wait_for_graph["c2"].add("c1")
        cycle = self.lm._detect_deadlock("c1")
        assert cycle is not None
        assert "c1" in cycle and "c2" in cycle


class TestConsistentHashRing:
    def test_add_node(self):
        ring = ConsistentHashRing(virtual_nodes=10)
        ring.add_node("http://node1:8001")
        assert len(ring.nodes) == 1
        assert len(ring.ring) == 10

    def test_get_node(self):
        ring = ConsistentHashRing(virtual_nodes=10)
        ring.add_node("http://node1:8001")
        ring.add_node("http://node2:8002")
        node = ring.get_node("test_topic")
        assert node in ["http://node1:8001", "http://node2:8002"]

    def test_distribution(self):
        ring = ConsistentHashRing(virtual_nodes=150)
        ring.add_node("http://n1:8001")
        ring.add_node("http://n2:8002")
        ring.add_node("http://n3:8003")
        counts = {"http://n1:8001": 0, "http://n2:8002": 0, "http://n3:8003": 0}
        for i in range(300):
            node = ring.get_node(f"topic_{i}")
            counts[node] += 1
        # Each node should get roughly 100 topics (allow 50% variance)
        for count in counts.values():
            assert 30 < count < 200

    def test_remove_node(self):
        ring = ConsistentHashRing(virtual_nodes=10)
        ring.add_node("http://n1:8001")
        ring.add_node("http://n2:8002")
        ring.remove_node("http://n1:8001")
        assert len(ring.nodes) == 1
        node = ring.get_node("any_key")
        assert node == "http://n2:8002"


class TestMESICache:
    def setup_method(self):
        self.metrics = MetricsCollector("test")

    @pytest.mark.asyncio
    async def test_write_creates_modified(self):
        cache = MESICache("n1", "http://n1:8001", [], max_size=10, metrics=self.metrics)
        await cache.start()
        result = await cache.write("k1", "v1")
        assert result["state"] == "M"
        assert cache.cache["k1"].state == CacheState.MODIFIED
        await cache.stop()

    @pytest.mark.asyncio
    async def test_lru_eviction(self):
        cache = MESICache("n1", "http://n1:8001", [], max_size=3, metrics=self.metrics)
        await cache.start()
        await cache.write("k1", "v1")
        await cache.write("k2", "v2")
        await cache.write("k3", "v3")
        await cache.write("k4", "v4")  # Should evict k1
        assert "k1" not in cache.cache
        assert len(cache.cache) == 3
        await cache.stop()

    def test_snoop_invalidate(self):
        cache = MESICache("n1", "http://n1:8001", [], max_size=10)
        cache.cache["k1"] = __import__('src.nodes.cache_node', fromlist=['CacheLine']).CacheLine(
            key="k1", value="v1", state=CacheState.SHARED
        )
        result = cache.handle_snoop_invalidate("k1", "n2")
        assert result["invalidated"]
        assert cache.cache["k1"].state == CacheState.INVALID

    def test_snoop_read_m_to_s(self):
        from src.nodes.cache_node import CacheLine
        cache = MESICache("n1", "http://n1:8001", [], max_size=10)
        cache.cache["k1"] = CacheLine(key="k1", value="v1", state=CacheState.MODIFIED)
        result = cache.handle_snoop_read("k1", "n2")
        assert result["has_data"]
        assert cache.cache["k1"].state == CacheState.SHARED

    def test_cache_stats(self):
        cache = MESICache("n1", "http://n1:8001", [], max_size=10, metrics=self.metrics)
        stats = cache.get_stats()
        assert "hit_rate_pct" in stats
        assert "state_distribution" in stats


class TestMetrics:
    def test_request_recording(self):
        m = MetricsCollector("test")
        m.record_request(5.0)
        m.record_request(10.0)
        assert m.request_count == 2
        stats = m.get_latency_stats()
        assert stats["avg_ms"] == 7.5

    def test_all_metrics(self):
        m = MetricsCollector("test")
        result = m.get_all_metrics()
        assert "lock" in result
        assert "queue" in result
        assert "cache" in result
        assert "raft" in result
