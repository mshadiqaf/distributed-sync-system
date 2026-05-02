"""
Base FastAPI node for the distributed synchronization system.
Provides health check, peer discovery, metrics, and cluster status endpoints.
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import time
import logging

from src.utils.config import get_settings
from src.utils.metrics import init_metrics, get_metrics
from src.utils.security import SecurityManager, SecurityMiddleware
from src.communication.message_passing import NodeClient
from src.communication.failure_detector import FailureDetector
from src.consensus.raft import RaftNode
from src.nodes.lock_manager import DistributedLockManager
from src.nodes.queue_node import DistributedQueue
from src.nodes.cache_node import MESICache

logger = logging.getLogger(__name__)

# Global references for shared components
node_client: NodeClient = None
failure_detector: FailureDetector = None
raft_node: RaftNode = None
lock_manager: DistributedLockManager = None
queue_system: DistributedQueue = None
cache_system: MESICache = None


def create_app() -> FastAPI:
    """Create and configure a FastAPI application for a distributed node."""

    global node_client, failure_detector, raft_node, lock_manager, queue_system, cache_system

    settings = get_settings()
    metrics = init_metrics(settings.node_id)
    peers = settings.get_peer_list()

    # Initialize communication components
    node_client = NodeClient(settings.node_id, peers, settings.node_secret)
    failure_detector = FailureDetector(
        node_id=settings.node_id,
        peers=peers,
        check_interval=2.0,
        failure_threshold=3,
    )

    # Initialize Raft consensus
    raft_node = RaftNode(
        node_id=settings.node_id,
        peers=peers,
        node_client=node_client,
        redis_url=settings.redis_url,
        election_timeout_min=settings.election_timeout_min,
        election_timeout_max=settings.election_timeout_max,
        heartbeat_interval=settings.heartbeat_interval,
    )

    # Initialize Lock Manager
    lock_manager = DistributedLockManager(
        node_id=settings.node_id,
        raft_node=raft_node,
        metrics=metrics,
    )

    # Initialize Queue System
    node_url = f"http://localhost:{settings.node_port}"
    queue_system = DistributedQueue(
        node_id=settings.node_id,
        node_url=node_url,
        peers=peers,
        node_client=node_client,
        redis_url=settings.redis_url,
        virtual_nodes=settings.queue_virtual_nodes,
        ack_timeout=settings.queue_ack_timeout,
        metrics=metrics,
    )

    # Initialize MESI Cache
    cache_system = MESICache(
        node_id=settings.node_id,
        node_url=node_url,
        peers=peers,
        node_client=node_client,
        redis_url=settings.redis_url,
        max_size=settings.cache_max_size,
        metrics=metrics,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Application startup and shutdown events."""
        logger.info(f"Node {settings.node_id} starting on port {settings.node_port}")
        logger.info(f"Peers: {peers}")

        # Start failure detector and Raft
        await failure_detector.start()
        await raft_node.start()
        await lock_manager.start()
        await queue_system.start()
        await cache_system.start()

        yield

        # Cleanup
        await cache_system.stop()
        await queue_system.stop()
        await lock_manager.stop()
        await raft_node.stop()
        await failure_detector.stop()
        await node_client.close()
        logger.info(f"Node {settings.node_id} shutting down")

    app = FastAPI(
        title=f"Distributed Sync Node - {settings.node_id}",
        description="Distributed Synchronization System Node",
        version="1.0.0",
        lifespan=lifespan,
    )

    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Security middleware (after CORS so preflight works)
    security_manager = SecurityManager(
        api_key=settings.api_key,
        admin_key=settings.admin_key,
        node_secret=settings.node_secret,
    )
    app.add_middleware(SecurityMiddleware, security_manager=security_manager)

    # Request timing middleware
    @app.middleware("http")
    async def track_request_metrics(request: Request, call_next):
        start = time.time()
        response = await call_next(request)
        latency_ms = (time.time() - start) * 1000
        metrics.record_request(latency_ms)
        return response

    # --- Health & Discovery Endpoints ---

    @app.get("/health", tags=["System"])
    async def health_check():
        """Check node health and basic information."""
        return {
            "node_id": settings.node_id,
            "status": "healthy",
            "uptime_seconds": round(metrics.uptime, 2),
            "port": settings.node_port,
            "role": raft_node.role.value if raft_node else "initializing",
            "leader_id": raft_node.leader_id if raft_node else None,
        }

    @app.get("/peers", tags=["System"])
    async def get_peers():
        """List all known peer nodes."""
        return {
            "node_id": settings.node_id,
            "peers": settings.get_peer_list(),
            "peer_count": len(settings.get_peer_list()),
        }

    @app.get("/metrics", tags=["System"])
    async def get_node_metrics():
        """Get comprehensive node metrics."""
        return metrics.get_all_metrics()

    @app.get("/cluster/status", tags=["System"])
    async def get_cluster_status():
        """Get health status of all nodes in the cluster."""
        return {
            "node_id": settings.node_id,
            "cluster": failure_detector.get_cluster_status(),
            "alive_peers": failure_detector.get_alive_peers(),
            "total_peers": len(peers),
        }

    # --- Raft Consensus Endpoints ---

    @app.post("/raft/request-vote", tags=["Raft"])
    async def request_vote(request: Request):
        """Handle RequestVote RPC from Raft candidates."""
        data = await request.json()
        return await raft_node.handle_request_vote(data)

    @app.post("/raft/append-entries", tags=["Raft"])
    async def append_entries(request: Request):
        """Handle AppendEntries RPC (heartbeat + log replication)."""
        data = await request.json()
        return await raft_node.handle_append_entries(data)

    @app.get("/raft/state", tags=["Raft"])
    async def get_raft_state():
        """Get current Raft consensus state."""
        return raft_node.get_state()

    # --- Lock Manager Endpoints ---

    @app.post("/lock/acquire", tags=["Lock Manager"])
    async def acquire_lock(request: Request):
        """Acquire a distributed lock on a resource."""
        data = await request.json()
        return await lock_manager.acquire_lock(
            resource=data.get("resource", ""),
            client_id=data.get("client_id", ""),
            lock_type=data.get("lock_type", "exclusive"),
            ttl=data.get("ttl", 30.0),
            timeout=data.get("timeout", 10.0),
        )

    @app.post("/lock/release", tags=["Lock Manager"])
    async def release_lock(request: Request):
        """Release a distributed lock."""
        data = await request.json()
        return await lock_manager.release_lock(
            resource=data.get("resource", ""),
            client_id=data.get("client_id", ""),
        )

    @app.get("/lock/status", tags=["Lock Manager"])
    async def get_lock_status():
        """Get all active locks and waiting requests."""
        return lock_manager.get_status()

    @app.get("/lock/deadlocks", tags=["Lock Manager"])
    async def get_deadlock_info():
        """Get current wait-for graph for deadlock analysis."""
        return lock_manager.get_deadlock_info()

    # --- Queue System Endpoints ---

    @app.post("/queue/push", tags=["Queue System"])
    async def push_message(request: Request):
        """Push a message to a distributed queue topic."""
        data = await request.json()
        return await queue_system.push_message(
            topic=data.get("topic", "default"),
            data=data.get("data"),
            producer_id=data.get("producer_id", "anonymous"),
            priority=data.get("priority", 0),
        )

    @app.post("/queue/consume", tags=["Queue System"])
    async def consume_message(request: Request):
        """Consume a message from a queue topic."""
        data = await request.json()
        return await queue_system.consume_message(
            topic=data.get("topic", "default"),
            consumer_id=data.get("consumer_id", "anonymous"),
        )

    @app.post("/queue/ack", tags=["Queue System"])
    async def ack_message(request: Request):
        """Acknowledge a consumed message."""
        data = await request.json()
        return await queue_system.ack_message(
            message_id=data.get("message_id", ""),
            consumer_id=data.get("consumer_id", "anonymous"),
        )

    @app.get("/queue/status", tags=["Queue System"])
    async def get_queue_status():
        """Get queue system status and topic information."""
        return queue_system.get_status()

    @app.get("/queue/ring", tags=["Queue System"])
    async def get_hash_ring():
        """Get consistent hash ring visualization."""
        return queue_system.hash_ring.get_ring_info()

    # --- Cache Coherence Endpoints ---

    @app.get("/cache/{key}", tags=["Cache (MESI)"])
    async def cache_read(key: str):
        """Read a value from the distributed cache (MESI protocol)."""
        return await cache_system.read(key)

    @app.put("/cache/{key}", tags=["Cache (MESI)"])
    async def cache_write(key: str, request: Request):
        """Write a value to the distributed cache (MESI protocol)."""
        data = await request.json()
        return await cache_system.write(key, data.get("value"))

    @app.delete("/cache/{key}", tags=["Cache (MESI)"])
    async def cache_delete(key: str):
        """Invalidate a cache entry."""
        return await cache_system.invalidate(key)

    @app.get("/cache-stats", tags=["Cache (MESI)"])
    async def cache_stats():
        """Get cache statistics and MESI state distribution."""
        return cache_system.get_stats()

    @app.get("/cache-entries", tags=["Cache (MESI)"])
    async def cache_entries():
        """Get all cache entries with MESI states."""
        return cache_system.get_entries()

    # --- Cache Snoop Protocol (Internal) ---

    @app.post("/cache/snoop/read", tags=["Cache Snoop"])
    async def snoop_read(request: Request):
        """Handle snoop read (BusRd) from a peer node."""
        data = await request.json()
        return cache_system.handle_snoop_read(
            key=data.get("key", ""),
            requester=data.get("requester", ""),
        )

    @app.post("/cache/snoop/invalidate", tags=["Cache Snoop"])
    async def snoop_invalidate(request: Request):
        """Handle snoop invalidate (BusRdX) from a peer node."""
        data = await request.json()
        return cache_system.handle_snoop_invalidate(
            key=data.get("key", ""),
            requester=data.get("requester", ""),
        )

    # --- Security & Audit Endpoints ---

    @app.get("/audit/logs", tags=["Security"])
    async def get_audit_logs():
        """Get recent audit log entries (admin only)."""
        return {
            "entries": security_manager.get_audit_log(limit=100),
            "total": len(security_manager.audit_log),
        }

    return app
