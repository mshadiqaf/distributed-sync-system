"""
Distributed Lock Manager with support for shared and exclusive locks.

Features:
- Shared (read) and Exclusive (write) locks
- Lock acquisition through Raft consensus
- TTL-based auto-release to prevent stale locks
- Deadlock detection using wait-for graph cycle detection
- Lock queuing for fair ordering
"""

import asyncio
import time
import logging
from typing import Dict, Any, List, Optional, Set, Tuple
from enum import Enum
from dataclasses import dataclass, field
from collections import defaultdict

logger = logging.getLogger(__name__)


class LockType(str, Enum):
    """Types of distributed locks."""
    SHARED = "shared"       # Multiple readers allowed
    EXCLUSIVE = "exclusive"  # Only one writer allowed


class LockStatus(str, Enum):
    """Status of a lock request."""
    GRANTED = "granted"
    WAITING = "waiting"
    DENIED = "denied"
    RELEASED = "released"
    TIMEOUT = "timeout"
    DEADLOCK = "deadlock"


@dataclass
class LockRequest:
    """Represents a lock acquisition request."""
    resource: str
    client_id: str
    lock_type: LockType
    timestamp: float = field(default_factory=time.time)
    ttl: float = 30.0  # seconds
    granted: bool = False
    granted_at: Optional[float] = None

    def is_expired(self) -> bool:
        """Check if a granted lock has expired."""
        if self.granted and self.granted_at:
            return (time.time() - self.granted_at) > self.ttl
        return False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "resource": self.resource,
            "client_id": self.client_id,
            "lock_type": self.lock_type.value,
            "timestamp": self.timestamp,
            "ttl": self.ttl,
            "granted": self.granted,
            "granted_at": self.granted_at,
            "expired": self.is_expired(),
        }


class DistributedLockManager:
    """
    Manages distributed locks with shared/exclusive semantics.

    Lock operations are replicated through Raft consensus to ensure
    consistency across the cluster.
    """

    def __init__(self, node_id: str, raft_node=None, metrics=None):
        self.node_id = node_id
        self.raft_node = raft_node
        self.metrics = metrics

        # Lock state
        # resource -> list of granted LockRequests
        self.granted_locks: Dict[str, List[LockRequest]] = defaultdict(list)
        # resource -> list of waiting LockRequests
        self.waiting_locks: Dict[str, List[LockRequest]] = defaultdict(list)

        # Wait-for graph for deadlock detection
        # client_id -> set of client_ids it's waiting for
        self.wait_for_graph: Dict[str, Set[str]] = defaultdict(set)

        # Background task for TTL cleanup and deadlock detection
        self._running = False
        self._cleanup_task: Optional[asyncio.Task] = None

    async def start(self):
        """Start background tasks."""
        self._running = True
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info(f"[{self.node_id}] Lock Manager started")

    async def stop(self):
        """Stop background tasks."""
        self._running = False
        if self._cleanup_task:
            self._cleanup_task.cancel()
        logger.info(f"[{self.node_id}] Lock Manager stopped")

    # --- Lock Operations ---

    async def acquire_lock(
        self,
        resource: str,
        client_id: str,
        lock_type: str = "exclusive",
        ttl: float = 30.0,
        timeout: float = 10.0,
    ) -> Dict[str, Any]:
        """
        Acquire a lock on a resource.

        For shared locks: multiple clients can hold simultaneously.
        For exclusive locks: only one client can hold at a time.
        """
        lt = LockType(lock_type)
        request = LockRequest(
            resource=resource,
            client_id=client_id,
            lock_type=lt,
            ttl=ttl,
        )

        # Check if Raft leader (only leader processes lock requests)
        if self.raft_node and self.raft_node.role.value != "leader":
            return {
                "status": LockStatus.DENIED.value,
                "reason": "not_leader",
                "leader_id": self.raft_node.leader_id,
                "message": f"Forward request to leader: {self.raft_node.leader_id}",
            }

        # Try to grant immediately
        can_grant = self._can_grant(resource, lt, client_id)

        if can_grant:
            request.granted = True
            request.granted_at = time.time()
            self.granted_locks[resource].append(request)

            # Propose to Raft for replication
            if self.raft_node:
                await self.raft_node.propose("lock_acquire", {
                    "resource": resource,
                    "client_id": client_id,
                    "lock_type": lock_type,
                    "ttl": ttl,
                })

            if self.metrics:
                self.metrics.lock_metrics["acquired"] += 1
                self.metrics.lock_metrics["active_locks"] = self._count_active_locks()

            logger.info(f"[{self.node_id}] Lock GRANTED: {client_id} -> {resource} ({lock_type})")
            return {
                "status": LockStatus.GRANTED.value,
                "resource": resource,
                "client_id": client_id,
                "lock_type": lock_type,
                "granted_at": request.granted_at,
                "ttl": ttl,
            }

        # Cannot grant immediately - add to wait queue
        self.waiting_locks[resource].append(request)
        self._update_wait_for_graph(resource, client_id)

        # Check for deadlocks
        deadlock = self._detect_deadlock(client_id)
        if deadlock:
            # Remove from wait queue
            self.waiting_locks[resource] = [
                w for w in self.waiting_locks[resource]
                if w.client_id != client_id
            ]
            self._clean_wait_for_graph(client_id)

            if self.metrics:
                self.metrics.lock_metrics["deadlocks_detected"] += 1

            logger.warning(f"[{self.node_id}] DEADLOCK detected: {deadlock}")
            return {
                "status": LockStatus.DEADLOCK.value,
                "reason": "deadlock_detected",
                "cycle": deadlock,
                "message": "Deadlock detected, request aborted",
            }

        # Wait for lock with timeout
        start_time = time.time()
        while (time.time() - start_time) < timeout:
            if self._can_grant(resource, lt, client_id):
                # Remove from wait queue
                self.waiting_locks[resource] = [
                    w for w in self.waiting_locks[resource]
                    if w.client_id != client_id
                ]
                self._clean_wait_for_graph(client_id)

                # Grant the lock
                request.granted = True
                request.granted_at = time.time()
                self.granted_locks[resource].append(request)

                if self.raft_node:
                    await self.raft_node.propose("lock_acquire", {
                        "resource": resource,
                        "client_id": client_id,
                        "lock_type": lock_type,
                        "ttl": ttl,
                    })

                if self.metrics:
                    self.metrics.lock_metrics["acquired"] += 1
                    self.metrics.lock_metrics["active_locks"] = self._count_active_locks()

                logger.info(f"[{self.node_id}] Lock GRANTED (waited): {client_id} -> {resource}")
                return {
                    "status": LockStatus.GRANTED.value,
                    "resource": resource,
                    "client_id": client_id,
                    "lock_type": lock_type,
                    "granted_at": request.granted_at,
                    "ttl": ttl,
                    "waited_ms": round((time.time() - start_time) * 1000, 2),
                }

            await asyncio.sleep(0.1)

        # Timeout - remove from wait queue
        self.waiting_locks[resource] = [
            w for w in self.waiting_locks[resource]
            if w.client_id != client_id
        ]
        self._clean_wait_for_graph(client_id)

        if self.metrics:
            self.metrics.lock_metrics["denied"] += 1

        logger.info(f"[{self.node_id}] Lock TIMEOUT: {client_id} -> {resource}")
        return {
            "status": LockStatus.TIMEOUT.value,
            "resource": resource,
            "client_id": client_id,
            "message": f"Lock acquisition timed out after {timeout}s",
        }

    async def release_lock(self, resource: str, client_id: str) -> Dict[str, Any]:
        """Release a lock on a resource."""
        # Find and remove the lock
        locks = self.granted_locks.get(resource, [])
        found = False
        for lock in locks:
            if lock.client_id == client_id:
                found = True
                break

        if not found:
            return {
                "status": "error",
                "message": f"No lock found for {client_id} on {resource}",
            }

        self.granted_locks[resource] = [
            l for l in locks if l.client_id != client_id
        ]

        # Clean up empty resources
        if not self.granted_locks[resource]:
            del self.granted_locks[resource]

        # Propose release to Raft
        if self.raft_node:
            await self.raft_node.propose("lock_release", {
                "resource": resource,
                "client_id": client_id,
            })

        if self.metrics:
            self.metrics.lock_metrics["released"] += 1
            self.metrics.lock_metrics["active_locks"] = self._count_active_locks()

        logger.info(f"[{self.node_id}] Lock RELEASED: {client_id} -> {resource}")
        return {
            "status": LockStatus.RELEASED.value,
            "resource": resource,
            "client_id": client_id,
        }

    # --- Lock Compatibility ---

    def _can_grant(self, resource: str, lock_type: LockType, client_id: str) -> bool:
        """Check if a lock can be granted based on current holders."""
        current_locks = self.granted_locks.get(resource, [])

        # Filter out expired locks
        current_locks = [l for l in current_locks if not l.is_expired()]
        self.granted_locks[resource] = current_locks

        if not current_locks:
            return True

        # Check if client already holds a lock on this resource
        for lock in current_locks:
            if lock.client_id == client_id:
                return True  # Re-entrant

        if lock_type == LockType.SHARED:
            # Shared lock: OK if all current holders are also shared
            return all(l.lock_type == LockType.SHARED for l in current_locks)
        else:
            # Exclusive lock: only if no one else holds any lock
            return False

    # --- Deadlock Detection ---

    def _update_wait_for_graph(self, resource: str, waiting_client: str):
        """Update the wait-for graph when a client starts waiting."""
        holders = self.granted_locks.get(resource, [])
        for holder in holders:
            if holder.client_id != waiting_client:
                self.wait_for_graph[waiting_client].add(holder.client_id)

    def _clean_wait_for_graph(self, client_id: str):
        """Remove a client from the wait-for graph."""
        self.wait_for_graph.pop(client_id, None)
        for waiter in self.wait_for_graph:
            self.wait_for_graph[waiter].discard(client_id)

    def _detect_deadlock(self, start_client: str) -> Optional[List[str]]:
        """
        Detect deadlock using DFS cycle detection in wait-for graph.

        Returns the cycle path if a deadlock is found, None otherwise.
        """
        visited = set()
        path = []

        def dfs(client: str) -> Optional[List[str]]:
            if client in visited:
                # Found a cycle
                cycle_start = path.index(client)
                return path[cycle_start:] + [client]

            visited.add(client)
            path.append(client)

            for waiting_for in self.wait_for_graph.get(client, set()):
                result = dfs(waiting_for)
                if result:
                    return result

            path.pop()
            return None

        return dfs(start_client)

    # --- Background Tasks ---

    async def _cleanup_loop(self):
        """Periodically clean up expired locks."""
        while self._running:
            try:
                self._cleanup_expired_locks()
                await asyncio.sleep(5.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.node_id}] Lock cleanup error: {e}")
                await asyncio.sleep(5.0)

    def _cleanup_expired_locks(self):
        """Remove expired locks and promote waiting requests."""
        for resource in list(self.granted_locks.keys()):
            locks = self.granted_locks[resource]
            expired = [l for l in locks if l.is_expired()]
            if expired:
                for lock in expired:
                    logger.info(
                        f"[{self.node_id}] Lock EXPIRED: {lock.client_id} -> {resource}"
                    )
                self.granted_locks[resource] = [
                    l for l in locks if not l.is_expired()
                ]
                if not self.granted_locks[resource]:
                    del self.granted_locks[resource]

    def _count_active_locks(self) -> int:
        """Count total active (non-expired) locks."""
        return sum(
            len([l for l in locks if not l.is_expired()])
            for locks in self.granted_locks.values()
        )

    # --- Status ---

    def get_status(self) -> Dict[str, Any]:
        """Get current lock manager status."""
        all_locks = []
        for resource, locks in self.granted_locks.items():
            for lock in locks:
                all_locks.append(lock.to_dict())

        all_waiting = []
        for resource, waiters in self.waiting_locks.items():
            for w in waiters:
                all_waiting.append(w.to_dict())

        return {
            "active_locks": all_locks,
            "active_count": len(all_locks),
            "waiting_requests": all_waiting,
            "waiting_count": len(all_waiting),
            "resources_locked": list(self.granted_locks.keys()),
        }

    def get_deadlock_info(self) -> Dict[str, Any]:
        """Get current wait-for graph for deadlock analysis."""
        return {
            "wait_for_graph": {
                k: list(v) for k, v in self.wait_for_graph.items()
            },
            "waiting_clients": list(self.wait_for_graph.keys()),
        }
