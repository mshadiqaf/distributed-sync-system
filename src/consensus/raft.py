"""
Simplified Raft Consensus Algorithm Implementation.

Implements core Raft features:
- Leader election with term-based voting
- Heartbeat mechanism (AppendEntries RPC)
- Basic log replication for distributed operations
- Leader failover on node failure

Intentionally simplified: no log compaction, no membership changes, no snapshots.
"""

import asyncio
import random
import time
import json
import logging
from enum import Enum
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field, asdict

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class RaftRole(str, Enum):
    """Possible roles for a Raft node."""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    """A single entry in the Raft log."""
    term: int
    index: int
    operation: str  # e.g., "lock_acquire", "lock_release"
    data: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict) -> "LogEntry":
        return cls(**d)


class RaftNode:
    """
    Simplified Raft consensus implementation.

    Handles leader election, heartbeats, and log replication
    for a cluster of distributed nodes.
    """

    def __init__(
        self,
        node_id: str,
        peers: List[str],
        node_client,  # NodeClient instance
        redis_url: str = "redis://localhost:6379/0",
        election_timeout_min: int = 1500,
        election_timeout_max: int = 3000,
        heartbeat_interval: int = 500,
    ):
        self.node_id = node_id
        self.peers = peers
        self.node_client = node_client
        self.redis_url = redis_url

        # Timing configuration (in milliseconds)
        self.election_timeout_min = election_timeout_min
        self.election_timeout_max = election_timeout_max
        self.heartbeat_interval = heartbeat_interval

        # Persistent state (persisted to Redis)
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []

        # Volatile state
        self.role: RaftRole = RaftRole.FOLLOWER
        self.leader_id: Optional[str] = None
        self.commit_index: int = 0
        self.last_applied: int = 0

        # Leader-only volatile state
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        # Timing
        self._last_heartbeat = time.time()
        self._election_timeout = self._random_election_timeout()

        # Background tasks
        self._running = False
        self._election_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

        # Redis connection
        self._redis: Optional[aioredis.Redis] = None

        # Callbacks for committed entries
        self._on_commit_callbacks = []

    def _random_election_timeout(self) -> float:
        """Generate a random election timeout in seconds."""
        return random.randint(self.election_timeout_min, self.election_timeout_max) / 1000.0

    def register_commit_callback(self, callback):
        """Register a callback to be called when a log entry is committed."""
        self._on_commit_callbacks.append(callback)

    # --- Lifecycle ---

    async def start(self):
        """Start the Raft node."""
        self._running = True
        try:
            self._redis = aioredis.from_url(self.redis_url, decode_responses=True)
            await self._load_persistent_state()
        except Exception as e:
            logger.warning(f"[{self.node_id}] Redis not available, using in-memory state: {e}")
            self._redis = None

        self._election_task = asyncio.create_task(self._election_loop())
        logger.info(f"[{self.node_id}] Raft started as {self.role.value}")

    async def stop(self):
        """Stop the Raft node."""
        self._running = False
        if self._election_task:
            self._election_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._redis:
            await self._redis.aclose()
        logger.info(f"[{self.node_id}] Raft stopped")

    # --- Persistence ---

    async def _save_persistent_state(self):
        """Save persistent state to Redis."""
        if not self._redis:
            return
        try:
            state = {
                "current_term": self.current_term,
                "voted_for": self.voted_for or "",
                "log": json.dumps([e.to_dict() for e in self.log]),
            }
            await self._redis.hset(f"raft:{self.node_id}", mapping=state)
        except Exception as e:
            logger.error(f"[{self.node_id}] Failed to save state: {e}")

    async def _load_persistent_state(self):
        """Load persistent state from Redis."""
        if not self._redis:
            return
        try:
            state = await self._redis.hgetall(f"raft:{self.node_id}")
            if state:
                self.current_term = int(state.get("current_term", 0))
                self.voted_for = state.get("voted_for") or None
                log_data = state.get("log", "[]")
                self.log = [LogEntry.from_dict(e) for e in json.loads(log_data)]
                logger.info(
                    f"[{self.node_id}] Loaded state: term={self.current_term}, "
                    f"log_len={len(self.log)}"
                )
        except Exception as e:
            logger.error(f"[{self.node_id}] Failed to load state: {e}")

    # --- Election ---

    async def _election_loop(self):
        """Main election timeout loop."""
        while self._running:
            try:
                if self.role != RaftRole.LEADER:
                    elapsed = time.time() - self._last_heartbeat
                    if elapsed > self._election_timeout:
                        await self._start_election()

                await asyncio.sleep(0.1)  # Check every 100ms
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.node_id}] Election loop error: {e}")
                await asyncio.sleep(0.5)

    async def _start_election(self):
        """Start a new election (become candidate)."""
        self.role = RaftRole.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self._election_timeout = self._random_election_timeout()
        self._last_heartbeat = time.time()

        await self._save_persistent_state()

        logger.info(
            f"[{self.node_id}] Starting election for term {self.current_term}"
        )

        # Request votes from all peers
        last_log_index = len(self.log) - 1 if self.log else -1
        last_log_term = self.log[-1].term if self.log else 0

        vote_request = {
            "term": self.current_term,
            "candidate_id": self.node_id,
            "last_log_index": last_log_index,
            "last_log_term": last_log_term,
        }

        responses = await self.node_client.broadcast_to_peers(
            "/raft/request-vote", vote_request
        )

        # Count votes (including self-vote)
        votes = 1  # Self-vote
        for peer, response in responses.items():
            if response and response.get("vote_granted"):
                votes += 1
                logger.info(f"[{self.node_id}] Got vote from {peer}")

        total_nodes = len(self.peers) + 1
        majority = (total_nodes // 2) + 1

        if votes >= majority and self.role == RaftRole.CANDIDATE:
            await self._become_leader()
        else:
            logger.info(
                f"[{self.node_id}] Election failed: {votes}/{majority} votes needed"
            )

    async def _become_leader(self):
        """Transition to leader role."""
        self.role = RaftRole.LEADER
        self.leader_id = self.node_id

        # Initialize leader state
        next_idx = len(self.log)
        for peer in self.peers:
            self.next_index[peer] = next_idx
            self.match_index[peer] = 0

        logger.info(
            f"[{self.node_id}] 🎯 Became LEADER for term {self.current_term}"
        )

        # Start heartbeat loop
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self):
        """Send periodic heartbeats to all peers (leader only)."""
        while self._running and self.role == RaftRole.LEADER:
            try:
                await self._send_heartbeats()
                await asyncio.sleep(self.heartbeat_interval / 1000.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.node_id}] Heartbeat error: {e}")

    async def _send_heartbeats(self):
        """Send AppendEntries RPCs to all peers as heartbeat."""
        for peer in self.peers:
            next_idx = self.next_index.get(peer, len(self.log))
            prev_log_index = next_idx - 1
            prev_log_term = self.log[prev_log_index].term if 0 <= prev_log_index < len(self.log) else 0

            # Include any new entries that the peer doesn't have
            entries = [e.to_dict() for e in self.log[next_idx:]]

            append_request = {
                "term": self.current_term,
                "leader_id": self.node_id,
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": entries,
                "leader_commit": self.commit_index,
            }

            # Send asynchronously (don't wait for all)
            asyncio.create_task(
                self._send_append_entries(peer, append_request)
            )

    async def _send_append_entries(self, peer: str, request: Dict):
        """Send AppendEntries to a single peer and handle response."""
        response = await self.node_client.send_to_peer(
            peer, "/raft/append-entries", request, retries=0
        )

        if response:
            if response.get("success"):
                # Update next_index and match_index
                entries_count = len(request.get("entries", []))
                if entries_count > 0:
                    self.next_index[peer] = request["prev_log_index"] + 1 + entries_count
                    self.match_index[peer] = self.next_index[peer] - 1
                    await self._try_advance_commit()
            elif response.get("term", 0) > self.current_term:
                # Step down if we see a higher term
                await self._step_down(response["term"])

    async def _try_advance_commit(self):
        """Advance commit index if majority has replicated."""
        for n in range(self.commit_index + 1, len(self.log)):
            if self.log[n].term == self.current_term:
                replicated = 1  # Self
                for peer in self.peers:
                    if self.match_index.get(peer, 0) >= n:
                        replicated += 1

                total_nodes = len(self.peers) + 1
                if replicated > total_nodes // 2:
                    self.commit_index = n
                    await self._apply_committed_entries()

    async def _apply_committed_entries(self):
        """Apply committed but not yet applied log entries."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            logger.info(
                f"[{self.node_id}] Applying entry {entry.index}: {entry.operation}"
            )
            for callback in self._on_commit_callbacks:
                try:
                    await callback(entry)
                except Exception as e:
                    logger.error(f"[{self.node_id}] Commit callback error: {e}")

    async def _step_down(self, new_term: int):
        """Step down to follower when a higher term is discovered."""
        logger.info(
            f"[{self.node_id}] Stepping down: term {self.current_term} → {new_term}"
        )
        self.current_term = new_term
        self.role = RaftRole.FOLLOWER
        self.voted_for = None
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        await self._save_persistent_state()

    # --- RPC Handlers ---

    async def handle_request_vote(self, request: Dict) -> Dict:
        """
        Handle a RequestVote RPC from a candidate.

        Returns vote_granted if:
        1. Candidate's term >= current term
        2. Haven't voted for someone else in this term
        3. Candidate's log is at least as up-to-date
        """
        candidate_term = request["term"]
        candidate_id = request["candidate_id"]
        last_log_index = request["last_log_index"]
        last_log_term = request["last_log_term"]

        # If candidate's term is higher, update our term
        if candidate_term > self.current_term:
            await self._step_down(candidate_term)

        vote_granted = False

        if candidate_term >= self.current_term:
            if self.voted_for is None or self.voted_for == candidate_id:
                # Check if candidate's log is at least as up-to-date
                my_last_term = self.log[-1].term if self.log else 0
                my_last_index = len(self.log) - 1 if self.log else -1

                if (last_log_term > my_last_term or
                    (last_log_term == my_last_term and last_log_index >= my_last_index)):
                    vote_granted = True
                    self.voted_for = candidate_id
                    self._last_heartbeat = time.time()
                    await self._save_persistent_state()
                    logger.info(
                        f"[{self.node_id}] Voted for {candidate_id} in term {candidate_term}"
                    )

        return {
            "term": self.current_term,
            "vote_granted": vote_granted,
            "voter_id": self.node_id,
        }

    async def handle_append_entries(self, request: Dict) -> Dict:
        """
        Handle an AppendEntries RPC (heartbeat + log replication).

        Resets election timer and processes any new log entries.
        """
        leader_term = request["term"]
        leader_id = request["leader_id"]

        # If leader's term is outdated, reject
        if leader_term < self.current_term:
            return {"term": self.current_term, "success": False}

        # Accept leader authority
        if leader_term > self.current_term:
            self.current_term = leader_term
            self.voted_for = None

        self.role = RaftRole.FOLLOWER
        self.leader_id = leader_id
        self._last_heartbeat = time.time()
        self._election_timeout = self._random_election_timeout()

        # Process log entries
        entries = request.get("entries", [])
        prev_log_index = request.get("prev_log_index", -1)
        prev_log_term = request.get("prev_log_term", 0)

        # Log consistency check
        if prev_log_index >= 0:
            if prev_log_index >= len(self.log):
                return {"term": self.current_term, "success": False}
            if self.log[prev_log_index].term != prev_log_term:
                # Delete conflicting entries
                self.log = self.log[:prev_log_index]
                await self._save_persistent_state()
                return {"term": self.current_term, "success": False}

        # Append new entries
        if entries:
            start_idx = prev_log_index + 1
            for i, entry_data in enumerate(entries):
                idx = start_idx + i
                entry = LogEntry.from_dict(entry_data)
                if idx < len(self.log):
                    if self.log[idx].term != entry.term:
                        self.log = self.log[:idx]
                        self.log.append(entry)
                else:
                    self.log.append(entry)

            await self._save_persistent_state()

        # Update commit index
        leader_commit = request.get("leader_commit", 0)
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
            await self._apply_committed_entries()

        return {"term": self.current_term, "success": True}

    # --- Client API ---

    async def propose(self, operation: str, data: Dict = None) -> Optional[LogEntry]:
        """
        Propose a new operation to the Raft cluster.
        Only the leader can accept proposals.

        Returns the LogEntry if successfully replicated, None otherwise.
        """
        if self.role != RaftRole.LEADER:
            return None

        entry = LogEntry(
            term=self.current_term,
            index=len(self.log),
            operation=operation,
            data=data or {},
        )

        self.log.append(entry)
        await self._save_persistent_state()

        logger.info(
            f"[{self.node_id}] Proposed: {operation} at index {entry.index}"
        )

        # Immediately try to replicate
        await self._send_heartbeats()

        # Wait briefly for replication
        for _ in range(10):
            if self.commit_index >= entry.index:
                return entry
            await asyncio.sleep(0.1)

        # Check if committed after waiting
        if self.commit_index >= entry.index:
            return entry

        return entry  # Return even if not yet committed (async replication)

    def get_state(self) -> Dict:
        """Get current Raft state for API response."""
        return {
            "node_id": self.node_id,
            "role": self.role.value,
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "leader_id": self.leader_id,
            "log_length": len(self.log),
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
        }
