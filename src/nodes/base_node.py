"""
Base FastAPI node for the distributed synchronization system.
Provides health check, peer discovery, and metrics endpoints.
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import time
import logging

from src.utils.config import get_settings
from src.utils.metrics import init_metrics, get_metrics

logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    """Create and configure a FastAPI application for a distributed node."""

    settings = get_settings()
    metrics = init_metrics(settings.node_id)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Application startup and shutdown events."""
        logger.info(f"Node {settings.node_id} starting on port {settings.node_port}")
        logger.info(f"Peers: {settings.get_peer_list()}")
        yield
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
            "role": metrics.raft_metrics.get("role", "initializing"),
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

    return app
