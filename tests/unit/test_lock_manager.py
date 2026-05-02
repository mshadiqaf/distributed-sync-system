"""Unit test untuk Distributed Lock Manager."""

import pytest
import asyncio
from src.nodes.lock_manager import DistributedLockManager, LockType, LockStatus
from src.utils.metrics import MetricsCollector


@pytest.fixture
def lm():
    metrics = MetricsCollector("test")
    return DistributedLockManager("test_node", raft_node=None, metrics=metrics)


class TestExclusiveLock:
    @pytest.mark.asyncio
    async def test_acquire_exclusive_granted(self, lm):
        r = await lm.acquire_lock("res", "c1", "exclusive", ttl=10)
        assert r["status"] == LockStatus.GRANTED.value

    @pytest.mark.asyncio
    async def test_exclusive_blocks_second_exclusive(self, lm):
        await lm.acquire_lock("res", "c1", "exclusive", ttl=60)
        r = await lm.acquire_lock("res", "c2", "exclusive", timeout=0.3)
        assert r["status"] == LockStatus.TIMEOUT.value

    @pytest.mark.asyncio
    async def test_exclusive_blocks_shared(self, lm):
        await lm.acquire_lock("res", "c1", "exclusive", ttl=60)
        r = await lm.acquire_lock("res", "c2", "shared", timeout=0.3)
        assert r["status"] == LockStatus.TIMEOUT.value

    @pytest.mark.asyncio
    async def test_exclusive_granted_after_release(self, lm):
        await lm.acquire_lock("res", "c1", "exclusive", ttl=60)
        await lm.release_lock("res", "c1")
        r = await lm.acquire_lock("res", "c2", "exclusive", timeout=2)
        assert r["status"] == LockStatus.GRANTED.value

    @pytest.mark.asyncio
    async def test_reentrant_same_client(self, lm):
        await lm.acquire_lock("res", "c1", "exclusive", ttl=60)
        r = await lm.acquire_lock("res", "c1", "exclusive", timeout=0.3)
        assert r["status"] == LockStatus.GRANTED.value


class TestSharedLock:
    @pytest.mark.asyncio
    async def test_two_shared_compatible(self, lm):
        r1 = await lm.acquire_lock("res", "c1", "shared", ttl=30)
        r2 = await lm.acquire_lock("res", "c2", "shared", ttl=30)
        assert r1["status"] == LockStatus.GRANTED.value
        assert r2["status"] == LockStatus.GRANTED.value

    @pytest.mark.asyncio
    async def test_three_shared_compatible(self, lm):
        results = []
        for i in range(3):
            r = await lm.acquire_lock("res", f"c{i}", "shared", ttl=30)
            results.append(r["status"])
        assert all(s == LockStatus.GRANTED.value for s in results)

    @pytest.mark.asyncio
    async def test_shared_blocks_exclusive(self, lm):
        await lm.acquire_lock("res", "c1", "shared", ttl=60)
        r = await lm.acquire_lock("res", "c2", "exclusive", timeout=0.3)
        assert r["status"] == LockStatus.TIMEOUT.value


class TestRelease:
    @pytest.mark.asyncio
    async def test_release_existing_lock(self, lm):
        await lm.acquire_lock("res", "c1", "exclusive")
        r = await lm.release_lock("res", "c1")
        assert r["status"] == LockStatus.RELEASED.value

    @pytest.mark.asyncio
    async def test_release_nonexistent_returns_error(self, lm):
        r = await lm.release_lock("res_nonexistent", "c1")
        assert r["status"] == "error"

    @pytest.mark.asyncio
    async def test_release_reduces_active_count(self, lm):
        await lm.acquire_lock("res", "c1", "exclusive")
        assert lm.get_status()["active_count"] == 1
        await lm.release_lock("res", "c1")
        assert lm.get_status()["active_count"] == 0


class TestDeadlockDetection:
    def test_simple_cycle_detected(self, lm):
        lm.wait_for_graph["c1"].add("c2")
        lm.wait_for_graph["c2"].add("c1")
        cycle = lm._detect_deadlock("c1")
        assert cycle is not None
        assert "c1" in cycle
        assert "c2" in cycle

    def test_three_node_cycle(self, lm):
        lm.wait_for_graph["c1"].add("c2")
        lm.wait_for_graph["c2"].add("c3")
        lm.wait_for_graph["c3"].add("c1")
        cycle = lm._detect_deadlock("c1")
        assert cycle is not None

    def test_no_cycle(self, lm):
        lm.wait_for_graph["c1"].add("c2")
        lm.wait_for_graph["c2"].add("c3")
        cycle = lm._detect_deadlock("c1")
        assert cycle is None

    def test_empty_graph_no_cycle(self, lm):
        cycle = lm._detect_deadlock("c1")
        assert cycle is None


class TestStatus:
    @pytest.mark.asyncio
    async def test_status_shows_active_locks(self, lm):
        await lm.acquire_lock("r1", "c1", "exclusive")
        await lm.acquire_lock("r2", "c2", "shared")
        status = lm.get_status()
        assert status["active_count"] == 2
        assert "r1" in status["resources_locked"]
        assert "r2" in status["resources_locked"]

    def test_deadlock_info_structure(self, lm):
        info = lm.get_deadlock_info()
        assert "wait_for_graph" in info
        assert "waiting_clients" in info
