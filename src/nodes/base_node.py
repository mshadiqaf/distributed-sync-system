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
from src.communication.message_passing import NodeClient
from src.communication.failure_detector import FailureDetector
from src.consensus.raft import RaftNode
from src.nodes.lock_manager import DistributedLockManager

logger = logging.getLogger(__name__)

# Global references for shared components
node_client: NodeClient = None
failure_detector: FailureDetector = None
raft_node: RaftNode = None
lock_manager: DistributedLockManager = None


def create_app() -> FastAPI:
    """Create and configure a FastAPI application for a distributed node."""

    global node_client, failure_detector, raft_node, lock_manager

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

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Application startup and shutdown events."""
        logger.info(f"Node {settings.node_id} starting on port {settings.node_port}")
        logger.info(f"Peers: {peers}")

        # Start failure detector and Raft
        await failure_detector.start()
        await raft_node.start()
        await lock_manager.start()

        yield

        # Cleanup
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

    return app
