"""
Failure detector for distributed node health monitoring.
Uses periodic heartbeat checks to detect node failures.
"""

import asyncio
import time
import logging
from typing import Dict, Callable, Optional, List
from enum import Enum

import httpx

logger = logging.getLogger(__name__)


class NodeStatus(str, Enum):
    """Possible states of a peer node."""
    ALIVE = "alive"
    SUSPECTED = "suspected"
    DEAD = "dead"
    UNKNOWN = "unknown"


class FailureDetector:
    """
    Detects peer node failures via periodic health checks.
    Uses a simple heartbeat-based approach with configurable intervals.
    """

    def __init__(
        self,
        node_id: str,
        peers: List[str],
        check_interval: float = 2.0,
        failure_threshold: int = 3,
        on_status_change: Optional[Callable] = None,
    ):
        """
        Args:
            node_id: This node's identifier
            peers: List of peer URLs to monitor
            check_interval: Seconds between health checks
            failure_threshold: Consecutive failures before marking dead
            on_status_change: Callback when a node's status changes
        """
        self.node_id = node_id
        self.peers = peers
        self.check_interval = check_interval
        self.failure_threshold = failure_threshold
        self.on_status_change = on_status_change

        # State tracking
        self.peer_status: Dict[str, NodeStatus] = {
            peer: NodeStatus.UNKNOWN for peer in peers
        }
        self.consecutive_failures: Dict[str, int] = {
            peer: 0 for peer in peers
        }
        self.last_seen: Dict[str, float] = {}

        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._client: Optional[httpx.AsyncClient] = None

    async def start(self):
        """Start the failure detector background task."""
        if self._running:
            return

        self._running = True
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(2.0))
        self._task = asyncio.create_task(self._monitor_loop())
        logger.info(f"[{self.node_id}] Failure detector started, monitoring {len(self.peers)} peers")

    async def stop(self):
        """Stop the failure detector."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._client:
            await self._client.aclose()
        logger.info(f"[{self.node_id}] Failure detector stopped")

    async def _monitor_loop(self):
        """Main monitoring loop - checks all peers periodically."""
        while self._running:
            try:
                await self._check_all_peers()
                await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.node_id}] Monitor loop error: {e}")
                await asyncio.sleep(self.check_interval)

    async def _check_all_peers(self):
        """Check health of all peers concurrently."""
        tasks = {
            peer: self._check_peer(peer) for peer in self.peers
        }
        await asyncio.gather(*tasks.values(), return_exceptions=True)

    async def _check_peer(self, peer_url: str):
        """Check health of a single peer."""
        try:
            response = await self._client.get(f"{peer_url}/health")
            if response.status_code == 200:
                self.consecutive_failures[peer_url] = 0
                self.last_seen[peer_url] = time.time()
                self._update_status(peer_url, NodeStatus.ALIVE)
                return
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout):
            pass
        except Exception as e:
            logger.debug(f"[{self.node_id}] Health check error for {peer_url}: {e}")

        # Failure detected
        self.consecutive_failures[peer_url] = self.consecutive_failures.get(peer_url, 0) + 1
        failures = self.consecutive_failures[peer_url]

        if failures >= self.failure_threshold:
            self._update_status(peer_url, NodeStatus.DEAD)
        else:
            self._update_status(peer_url, NodeStatus.SUSPECTED)

    def _update_status(self, peer_url: str, new_status: NodeStatus):
        """Update peer status and trigger callback if changed."""
        old_status = self.peer_status.get(peer_url, NodeStatus.UNKNOWN)
        if old_status != new_status:
            self.peer_status[peer_url] = new_status
            logger.info(
                f"[{self.node_id}] Peer {peer_url}: {old_status.value} → {new_status.value}"
            )
            if self.on_status_change:
                try:
                    self.on_status_change(peer_url, old_status, new_status)
                except Exception as e:
                    logger.error(f"Status change callback error: {e}")
        else:
            self.peer_status[peer_url] = new_status

    def get_alive_peers(self) -> List[str]:
        """Get list of peers currently considered alive."""
        return [
            peer for peer, status in self.peer_status.items()
            if status == NodeStatus.ALIVE
        ]

    def get_cluster_status(self) -> Dict[str, Dict]:
        """Get full cluster status report."""
        return {
            peer: {
                "status": self.peer_status.get(peer, NodeStatus.UNKNOWN).value,
                "consecutive_failures": self.consecutive_failures.get(peer, 0),
                "last_seen": self.last_seen.get(peer),
            }
            for peer in self.peers
        }

    def add_peer(self, peer_url: str):
        """Add a new peer to monitor."""
        if peer_url not in self.peers:
            self.peers.append(peer_url)
            self.peer_status[peer_url] = NodeStatus.UNKNOWN
            self.consecutive_failures[peer_url] = 0

    def remove_peer(self, peer_url: str):
        """Remove a peer from monitoring."""
        self.peers = [p for p in self.peers if p != peer_url]
        self.peer_status.pop(peer_url, None)
        self.consecutive_failures.pop(peer_url, None)
        self.last_seen.pop(peer_url, None)
