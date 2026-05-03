"""
MESI Cache Coherence Protocol Implementation.

States:
- Modified (M): Data modified locally, only valid copy
- Exclusive (E): Data clean, only copy in this cache
- Shared (S): Data clean, may exist in other caches
- Invalid (I): Data is stale/not present

Features:
- Full MESI state machine with proper transitions
- Cache invalidation propagation across nodes
- LRU cache replacement policy
- Performance metrics (hit rate, miss rate, state distribution)
"""

import asyncio
import time
import logging
from typing import Any, Dict, Optional, List
from enum import Enum
from collections import OrderedDict
from dataclasses import dataclass, field

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class CacheState(str, Enum):
    """MESI cache line states."""
    MODIFIED = "M"
    EXCLUSIVE = "E"
    SHARED = "S"
    INVALID = "I"


@dataclass
class CacheLine:
    """A single cache entry with MESI state."""
    key: str
    value: Any
    state: CacheState
    last_accessed: float = field(default_factory=time.time)
    last_modified: float = field(default_factory=time.time)

    def to_dict(self) -> Dict:
        return {
            "key": self.key,
            "value": self.value,
            "state": self.state.value,
            "last_accessed": self.last_accessed,
            "last_modified": self.last_modified,
        }


class MESICache:
    """
    Distributed cache with MESI coherence protocol.

    Ensures cache consistency across multiple nodes by implementing
    the MESI state machine with bus-based invalidation.
    """

    def __init__(
        self,
        node_id: str,
        node_url: str,
        peers: List[str],
        node_client=None,
        redis_url: str = "redis://localhost:6379/0",
        max_size: int = 1000,
        metrics=None,
    ):
        self.node_id = node_id
        self.node_url = node_url
        self.peers = peers
        self.node_client = node_client
        self.redis_url = redis_url
        self.max_size = max_size
        self.metrics = metrics

        # LRU Cache: OrderedDict maintains insertion/access order
        self.cache: OrderedDict[str, CacheLine] = OrderedDict()

        # Redis connection for backing store
        self._redis: Optional[aioredis.Redis] = None
        self._running = False

    async def start(self):
        """Start the cache system."""
        self._running = True
        try:
            self._redis = aioredis.from_url(self.redis_url, decode_responses=True)
        except Exception as e:
            logger.warning(f"[{self.node_id}] Redis not available for cache: {e}")
            self._redis = None
        logger.info(f"[{self.node_id}] MESI Cache started (max_size={self.max_size})")

    async def stop(self):
        """Stop the cache, flush modified entries."""
        self._running = False
        # Flush all Modified entries to Redis before shutdown
        await self._flush_all_modified()
        if self._redis:
            await self._redis.close()
        logger.info(f"[{self.node_id}] MESI Cache stopped")

    # --- Read Operation ---

    async def read(self, key: str) -> Dict[str, Any]:
        """
        Read a value from the cache with MESI protocol.

        Flow:
        1. Check local cache
        2. If miss → check if any peer has it (BusRd)
        3. If no peer has it → fetch from Redis (backing store)
        """
        # Check local cache
        if key in self.cache:
            line = self.cache[key]
            if line.state != CacheState.INVALID:
                # Cache HIT
                line.last_accessed = time.time()
                self.cache.move_to_end(key)  # LRU update

                if self.metrics:
                    self.metrics.cache_metrics["hits"] += 1

                logger.debug(f"[{self.node_id}] Cache HIT: {key} (state={line.state.value})")
                return {
                    "status": "hit",
                    "key": key,
                    "value": line.value,
                    "state": line.state.value,
                    "node": self.node_id,
                }

        # Cache MISS
        if self.metrics:
            self.metrics.cache_metrics["misses"] += 1

        # BusRd: Check if any peer has the data
        peer_data = await self._bus_read(key)

        if peer_data is not None:
            # Peer has it → both transition to Shared
            self._insert_line(key, peer_data, CacheState.SHARED)
            logger.info(f"[{self.node_id}] Cache MISS → got from peer: {key} → Shared")
            return {
                "status": "miss_peer",
                "key": key,
                "value": peer_data,
                "state": CacheState.SHARED.value,
                "node": self.node_id,
            }

        # No peer has it → fetch from Redis (main memory)
        value = await self._read_from_store(key)
        if value is not None:
            self._insert_line(key, value, CacheState.EXCLUSIVE)
            logger.info(f"[{self.node_id}] Cache MISS → got from store: {key} → Exclusive")
            return {
                "status": "miss_store",
                "key": key,
                "value": value,
                "state": CacheState.EXCLUSIVE.value,
                "node": self.node_id,
            }

        logger.info(f"[{self.node_id}] Cache MISS → key not found: {key}")
        return {
            "status": "not_found",
            "key": key,
            "value": None,
            "node": self.node_id,
        }

    # --- Write Operation ---

    async def write(self, key: str, value: Any) -> Dict[str, Any]:
        """
        Write a value to the cache with MESI protocol.

        Flow:
        1. Send BusRdX/BusUpgr to invalidate other caches
        2. Update local cache → state = Modified
        3. (Write-back: flush to Redis on eviction or shutdown)
        """
        # Invalidate all other caches (BusRdX)
        await self._bus_invalidate(key)

        if key in self.cache:
            line = self.cache[key]
            old_state = line.state

            # State transitions for write hit
            # E → M: Silently upgrade (no bus needed, already only copy)
            # S → M: Need to invalidate others (done above)
            # M → M: Just update value
            line.value = value
            line.state = CacheState.MODIFIED
            line.last_modified = time.time()
            line.last_accessed = time.time()
            self.cache.move_to_end(key)

            logger.info(
                f"[{self.node_id}] Cache WRITE HIT: {key} ({old_state.value} → M)"
            )
        else:
            # Write miss → insert as Modified
            self._insert_line(key, value, CacheState.MODIFIED)
            logger.info(f"[{self.node_id}] Cache WRITE MISS: {key} → Modified")

        if self.metrics:
            self.metrics.cache_metrics["entries"] = len(self.cache)

        return {
            "status": "written",
            "key": key,
            "value": value,
            "state": CacheState.MODIFIED.value,
            "node": self.node_id,
        }

    # --- Delete/Invalidate Operation ---

    async def invalidate(self, key: str) -> Dict[str, Any]:
        """Invalidate a cache entry locally and propagate to peers."""
        await self._bus_invalidate(key)
        result = self._local_invalidate(key)

        # Also remove from backing store
        if self._redis:
            try:
                await self._redis.delete(f"cache_store:{key}")
            except Exception:
                pass

        return {
            "status": "invalidated",
            "key": key,
            "node": self.node_id,
            **result,
        }

    # --- Bus Operations (Inter-node communication) ---

    async def _bus_read(self, key: str) -> Optional[Any]:
        """
        BusRd: Ask peers if they have the data.
        If a peer has it in M state, they flush to memory and both go to S.
        If a peer has it in E state, they transition to S.
        If a peer has it in S state, no state change.
        """
        if not self.node_client:
            return None

        responses = await self.node_client.broadcast_to_peers(
            "/cache/snoop/read",
            {"key": key, "requester": self.node_id},
        )

        for peer, response in responses.items():
            if response and response.get("has_data"):
                return response.get("value")

        return None

    async def _bus_invalidate(self, key: str):
        """
        BusRdX/BusUpgr: Invalidate a key in all peer caches.
        """
        if not self.node_client:
            return

        if self.metrics:
            self.metrics.cache_metrics["invalidations"] += 1

        await self.node_client.broadcast_to_peers(
            "/cache/snoop/invalidate",
            {"key": key, "requester": self.node_id},
        )

    # --- Snoop Handlers (respond to bus operations from peers) ---

    def handle_snoop_read(self, key: str, requester: str) -> Dict[str, Any]:
        """
        Handle a snoop read request from another node.
        Transitions:
        - M → S (flush to memory first)
        - E → S
        - S → S (no change)
        """
        if key not in self.cache:
            return {"has_data": False}

        line = self.cache[key]
        if line.state == CacheState.INVALID:
            return {"has_data": False}

        value = line.value

        if line.state == CacheState.MODIFIED:
            # M → S: Flush to memory (Redis) first
            asyncio.create_task(self._write_to_store(key, value))
            line.state = CacheState.SHARED
            logger.info(f"[{self.node_id}] Snoop read: {key} (M → S), flushed to store")
        elif line.state == CacheState.EXCLUSIVE:
            # E → S
            line.state = CacheState.SHARED
            logger.info(f"[{self.node_id}] Snoop read: {key} (E → S)")

        return {"has_data": True, "value": value, "state": line.state.value}

    def handle_snoop_invalidate(self, key: str, requester: str) -> Dict[str, Any]:
        """
        Handle a snoop invalidate request from another node.
        All states → I
        If Modified, flush to memory first.
        """
        if key not in self.cache:
            return {"invalidated": False, "reason": "not_in_cache"}

        line = self.cache[key]
        old_state = line.state

        if old_state == CacheState.MODIFIED:
            # Flush modified data before invalidating
            asyncio.create_task(self._write_to_store(key, line.value))

        line.state = CacheState.INVALID
        logger.info(
            f"[{self.node_id}] Snoop invalidate: {key} ({old_state.value} → I)"
        )

        return {"invalidated": True, "old_state": old_state.value}

    # --- LRU Management ---

    def _insert_line(self, key: str, value: Any, state: CacheState):
        """Insert a cache line, evicting LRU entry if at capacity."""
        # Evict if at capacity
        while len(self.cache) >= self.max_size:
            evicted_key, evicted_line = self.cache.popitem(last=False)
            if evicted_line.state == CacheState.MODIFIED:
                # Write-back evicted modified data
                asyncio.create_task(self._write_to_store(evicted_key, evicted_line.value))
            if self.metrics:
                self.metrics.cache_metrics["evictions"] += 1
            logger.info(f"[{self.node_id}] LRU eviction: {evicted_key}")

        self.cache[key] = CacheLine(
            key=key,
            value=value,
            state=state,
        )

    def _local_invalidate(self, key: str) -> Dict:
        """Invalidate a key locally."""
        if key in self.cache:
            old_state = self.cache[key].state
            self.cache[key].state = CacheState.INVALID
            return {"was_cached": True, "old_state": old_state.value}
        return {"was_cached": False}

    # --- Backing Store (Redis) ---

    async def _read_from_store(self, key: str) -> Optional[Any]:
        """Read a value from the backing store (Redis)."""
        if not self._redis:
            return None
        try:
            value = await self._redis.get(f"cache_store:{key}")
            return value
        except Exception as e:
            logger.error(f"[{self.node_id}] Store read error: {e}")
            return None

    async def _write_to_store(self, key: str, value: Any):
        """Write a value to the backing store (Redis)."""
        if not self._redis:
            return
        try:
            await self._redis.set(f"cache_store:{key}", str(value))
        except Exception as e:
            logger.error(f"[{self.node_id}] Store write error: {e}")

    async def _flush_all_modified(self):
        """Flush all Modified entries to the backing store."""
        for key, line in self.cache.items():
            if line.state == CacheState.MODIFIED:
                await self._write_to_store(key, line.value)
                line.state = CacheState.EXCLUSIVE
                logger.info(f"[{self.node_id}] Flushed: {key} (M → E)")

    # --- Status & Metrics ---

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        state_counts = {"M": 0, "E": 0, "S": 0, "I": 0}
        for line in self.cache.values():
            state_counts[line.state.value] += 1

        total_accesses = 0
        hit_rate = 0.0
        if self.metrics:
            hits = self.metrics.cache_metrics["hits"]
            misses = self.metrics.cache_metrics["misses"]
            total_accesses = hits + misses
            hit_rate = round(hits / total_accesses * 100, 2) if total_accesses > 0 else 0

        return {
            "node_id": self.node_id,
            "total_entries": len(self.cache),
            "max_size": self.max_size,
            "utilization_pct": round(len(self.cache) / self.max_size * 100, 2),
            "state_distribution": state_counts,
            "hit_rate_pct": hit_rate,
            "total_accesses": total_accesses,
            "evictions": self.metrics.cache_metrics["evictions"] if self.metrics else 0,
            "invalidations": self.metrics.cache_metrics["invalidations"] if self.metrics else 0,
        }

    def get_entries(self) -> Dict[str, Any]:
        """Get all cache entries with their MESI states."""
        entries = []
        for key, line in self.cache.items():
            entries.append(line.to_dict())
        return {
            "node_id": self.node_id,
            "entries": entries,
            "count": len(entries),
        }
