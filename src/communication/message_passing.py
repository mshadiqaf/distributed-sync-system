"""
Async HTTP communication layer for inter-node messaging.
Provides reliable message passing with retry logic and timeout handling.
"""

import httpx
import asyncio
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Default timeout for inter-node requests (seconds)
DEFAULT_TIMEOUT = 5.0
MAX_RETRIES = 3
RETRY_BASE_DELAY = 0.1  # seconds


class NodeClient:
    """Async HTTP client for inter-node communication."""

    def __init__(self, node_id: str, peers: List[str], node_secret: str = ""):
        self.node_id = node_id
        self.peers = peers
        self.node_secret = node_secret
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the async HTTP client."""
        if self._client is None or self._client.is_closed:
            from src.utils.config import get_settings
            settings = get_settings()
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(DEFAULT_TIMEOUT),
                headers={
                    "X-Node-ID": self.node_id,
                    "X-Node-Secret": self.node_secret,
                    "X-Source-Region": settings.node_region,
                },
            )
        return self._client

    async def close(self):
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def send_to_peer(
        self,
        peer_url: str,
        path: str,
        data: Dict[str, Any] = None,
        method: str = "POST",
        retries: int = MAX_RETRIES,
        timeout: float = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Send a request to a specific peer with retry logic.

        Args:
            peer_url: Base URL of the peer node
            path: API endpoint path (e.g., '/raft/request-vote')
            data: Request body (JSON)
            method: HTTP method
            retries: Number of retry attempts

        Returns:
            Response JSON or None if failed
        """
        client = await self._get_client()
        url = f"{peer_url}{path}"

        for attempt in range(retries + 1):
            try:
                if method == "POST":
                    response = await client.post(url, json=data or {}, timeout=timeout)
                elif method == "GET":
                    response = await client.get(url, params=data, timeout=timeout)
                else:
                    response = await client.request(method, url, json=data, timeout=timeout)

                if response.status_code == 200:
                    return response.json()
                else:
                    logger.warning(
                        f"[{self.node_id}] Request to {url} returned {response.status_code}"
                    )
                    return None

            except httpx.RequestError as e:
                if attempt < retries:
                    delay = RETRY_BASE_DELAY * (2 ** attempt)
                    logger.debug(
                        f"[{self.node_id}] Retry {attempt + 1}/{retries} to {url} after {delay}s"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.warning(f"[{self.node_id}] Failed to reach {url}: {e}")
                    return None

            except Exception as e:
                logger.error(f"[{self.node_id}] Unexpected error to {url}: {e}")
                return None

        return None

    async def broadcast_to_peers(
        self,
        path: str,
        data: Dict[str, Any] = None,
        method: str = "POST",
        exclude: List[str] = None,
        timeout: float = None,
        retries: int = 1,
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Broadcast a request to all peers concurrently.

        Args:
            path: API endpoint path
            data: Request body
            method: HTTP method
            exclude: List of peer URLs to exclude

        Returns:
            Dict mapping peer URL to response (or None if failed)
        """
        exclude = exclude or []
        tasks = {}

        for peer in self.peers:
            if peer not in exclude:
                tasks[peer] = self.send_to_peer(
                    peer, path, data, method, retries=retries, timeout=timeout
                )

        if not tasks:
            return {}

        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        return {
            peer: (result if not isinstance(result, Exception) else None)
            for peer, result in zip(tasks.keys(), results)
        }

    async def send_to_leader(
        self,
        leader_url: str,
        path: str,
        data: Dict[str, Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Send a request specifically to the Raft leader."""
        return await self.send_to_peer(leader_url, path, data, retries=MAX_RETRIES)
