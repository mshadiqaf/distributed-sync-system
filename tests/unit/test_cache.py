"""Unit test untuk MESI Cache Coherence."""

import pytest
from src.nodes.cache_node import MESICache, CacheState, CacheLine
from src.utils.metrics import MetricsCollector


def make_cache(max_size=10, metrics=None):
    """Buat cache tanpa koneksi Redis (unit test only)."""
    c = MESICache("n1", "http://n1:8001", [], max_size=max_size, metrics=metrics)
    c._running = True  # bypass start() agar tidak perlu Redis
    return c


@pytest.fixture
def metrics():
    return MetricsCollector("test")


@pytest.fixture
def cache(metrics):
    return make_cache(max_size=10, metrics=metrics)


class TestWrite:
    @pytest.mark.asyncio
    async def test_write_new_key_creates_modified(self, cache):
        r = await cache.write("k1", "v1")
        assert r["state"] == "M"
        assert cache.cache["k1"].state == CacheState.MODIFIED

    @pytest.mark.asyncio
    async def test_write_updates_value(self, cache):
        await cache.write("k1", "v1")
        r = await cache.write("k1", "v2")
        assert r["state"] == "M"
        assert cache.cache["k1"].value == "v2"

    @pytest.mark.asyncio
    async def test_write_exclusive_to_modified(self, cache):
        cache.cache["k1"] = CacheLine(key="k1", value="v1", state=CacheState.EXCLUSIVE)
        r = await cache.write("k1", "v2")
        assert r["state"] == "M"

    @pytest.mark.asyncio
    async def test_write_shared_to_modified(self, cache):
        cache.cache["k1"] = CacheLine(key="k1", value="v1", state=CacheState.SHARED)
        r = await cache.write("k1", "v2")
        assert r["state"] == "M"


class TestRead:
    @pytest.mark.asyncio
    async def test_read_hit(self, cache):
        await cache.write("k1", "v1")
        r = await cache.read("k1")
        assert r["status"] == "hit"
        assert r["value"] == "v1"

    @pytest.mark.asyncio
    async def test_read_invalid_is_miss(self, cache):
        cache.cache["k1"] = CacheLine(key="k1", value="v1", state=CacheState.INVALID)
        r = await cache.read("k1")
        assert r["status"] != "hit"

    @pytest.mark.asyncio
    async def test_read_miss_not_found(self, cache):
        r = await cache.read("nonexistent")
        assert r["status"] == "not_found"

    @pytest.mark.asyncio
    async def test_read_hit_updates_metrics(self, cache, metrics):
        await cache.write("k1", "v1")
        initial_hits = metrics.cache_metrics["hits"]
        await cache.read("k1")
        assert metrics.cache_metrics["hits"] == initial_hits + 1

    @pytest.mark.asyncio
    async def test_read_miss_updates_metrics(self, cache, metrics):
        initial_misses = metrics.cache_metrics["misses"]
        await cache.read("missing_key")
        assert metrics.cache_metrics["misses"] == initial_misses + 1


class TestLRUEviction:
    @pytest.mark.asyncio
    async def test_evicts_lru_entry(self, metrics):
        c = make_cache(max_size=3, metrics=metrics)
        for i in range(4):
            await c.write(f"k{i}", f"v{i}")
        assert "k0" not in c.cache
        assert len(c.cache) == 3

    @pytest.mark.asyncio
    async def test_access_order_updates_lru(self, metrics):
        c = make_cache(max_size=3, metrics=metrics)
        await c.write("k1", "v1")
        await c.write("k2", "v2")
        await c.write("k3", "v3")
        await c.read("k1")
        await c.write("k4", "v4")
        assert "k1" in c.cache
        assert "k2" not in c.cache

    @pytest.mark.asyncio
    async def test_eviction_counter_increments(self, metrics):
        c = make_cache(max_size=2, metrics=metrics)
        for i in range(3):
            await c.write(f"k{i}", f"v{i}")
        assert metrics.cache_metrics["evictions"] >= 1


class TestSnoopHandlers:
    def test_snoop_read_shared_returns_data(self):
        c = make_cache()
        c.cache["k1"] = CacheLine(key="k1", value="v1", state=CacheState.SHARED)
        r = c.handle_snoop_read("k1", "n2")
        assert r["has_data"]
        assert r["value"] == "v1"
        assert c.cache["k1"].state == CacheState.SHARED

    def test_snoop_read_exclusive_to_shared(self):
        c = make_cache()
        c.cache["k1"] = CacheLine(key="k1", value="v1", state=CacheState.EXCLUSIVE)
        r = c.handle_snoop_read("k1", "n2")
        assert r["has_data"]
        assert c.cache["k1"].state == CacheState.SHARED

    @pytest.mark.asyncio
    async def test_snoop_read_modified_to_shared(self):
        c = make_cache()
        c.cache["k1"] = CacheLine(key="k1", value="v1", state=CacheState.MODIFIED)
        r = c.handle_snoop_read("k1", "n2")
        assert r["has_data"]
        assert c.cache["k1"].state == CacheState.SHARED

    def test_snoop_read_missing_key(self):
        c = make_cache()
        r = c.handle_snoop_read("nonexistent", "n2")
        assert not r["has_data"]

    def test_snoop_invalidate_shared_to_invalid(self):
        c = make_cache()
        c.cache["k1"] = CacheLine(key="k1", value="v1", state=CacheState.SHARED)
        r = c.handle_snoop_invalidate("k1", "n2")
        assert r["invalidated"]
        assert c.cache["k1"].state == CacheState.INVALID

    def test_snoop_invalidate_exclusive_to_invalid(self):
        c = make_cache()
        c.cache["k1"] = CacheLine(key="k1", value="v1", state=CacheState.EXCLUSIVE)
        r = c.handle_snoop_invalidate("k1", "n2")
        assert r["invalidated"]
        assert r["old_state"] == "E"

    def test_snoop_invalidate_missing_key(self):
        c = make_cache()
        r = c.handle_snoop_invalidate("nonexistent", "n2")
        assert not r["invalidated"]


class TestStats:
    def test_stats_structure(self, metrics):
        c = make_cache(metrics=metrics)
        stats = c.get_stats()
        assert "hit_rate_pct" in stats
        assert "state_distribution" in stats
        assert "total_entries" in stats
        assert "utilization_pct" in stats
        assert set(stats["state_distribution"].keys()) == {"M", "E", "S", "I"}

    def test_entries_structure(self):
        c = make_cache()
        c.cache["k1"] = CacheLine(key="k1", value="v1", state=CacheState.EXCLUSIVE)
        result = c.get_entries()
        assert result["count"] == 1
        assert result["entries"][0]["state"] == "E"
