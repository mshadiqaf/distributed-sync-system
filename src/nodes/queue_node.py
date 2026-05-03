"""
Distributed Queue System with consistent hashing.

Features:
- Consistent hashing with virtual nodes for even distribution
- Multiple producers and consumers per topic
- Message persistence via Redis
- At-least-once delivery guarantee with ack-based tracking
- Node failure recovery through hash ring rehashing
"""

import asyncio
import hashlib
import json
import time
import uuid
import logging
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field, asdict
from bisect import bisect_right
from collections import defaultdict

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


@dataclass
class Message:
    """A queue message."""
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    topic: str = ""
    data: Any = None
    producer_id: str = ""
    timestamp: float = field(default_factory=time.time)
    priority: int = 0
    delivery_count: int = 0
    acked: bool = False

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict) -> "Message":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


class ConsistentHashRing:
    """
    Consistent hash ring for distributing topics across nodes.
    Uses virtual nodes for better distribution.
    """

    def __init__(self, virtual_nodes: int = 150):
        self.virtual_nodes = virtual_nodes
        self.ring: List[Tuple[int, str]] = []  # (hash, node_id)
        self.nodes: set = set()

    def _hash(self, key: str) -> int:
        """Generate a consistent hash for a key."""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node_url: str):
        """Add a node to the hash ring with virtual nodes."""
        if node_url in self.nodes:
            return
        self.nodes.add(node_url)
        for i in range(self.virtual_nodes):
            vnode_key = f"{node_url}:vnode:{i}"
            h = self._hash(vnode_key)
            self.ring.append((h, node_url))
        self.ring.sort(key=lambda x: x[0])

    def remove_node(self, node_url: str):
        """Remove a node and its virtual nodes from the ring."""
        self.nodes.discard(node_url)
        self.ring = [(h, n) for h, n in self.ring if n != node_url]

    def get_node(self, key: str) -> Optional[str]:
        """Get the node responsible for a given key."""
        if not self.ring:
            return None
        h = self._hash(key)
        hashes = [r[0] for r in self.ring]
        idx = bisect_right(hashes, h) % len(self.ring)
        return self.ring[idx][1]

    def get_ring_info(self) -> Dict:
        """Get information about the hash ring."""
        node_counts = defaultdict(int)
        for _, node in self.ring:
            node_counts[node] += 1
        return {
            "total_vnodes": len(self.ring),
            "nodes": list(self.nodes),
            "node_count": len(self.nodes),
            "distribution": dict(node_counts),
        }


class DistributedQueue:
    """
    Distributed message queue using consistent hashing.

    Messages are routed to nodes based on topic hash.
    Persistence is handled via Redis.
    """

    def __init__(
        self,
        node_id: str,
        node_url: str,
        peers: List[str],
        node_client=None,
        redis_url: str = "redis://localhost:6379/0",
        virtual_nodes: int = 150,
        ack_timeout: float = 30.0,
        metrics=None,
    ):
        self.node_id = node_id
        self.node_url = node_url
        self.peers = peers
        self.node_client = node_client
        self.redis_url = redis_url
        self.ack_timeout = ack_timeout
        self.metrics = metrics

        # Hash ring
        self.hash_ring = ConsistentHashRing(virtual_nodes)
        self._init_hash_ring()

        # In-memory queue per topic (for topics owned by this node)
        self.queues: Dict[str, List[Message]] = defaultdict(list)

        # Messages delivered but not yet acked
        self.pending_acks: Dict[str, Message] = {}  # message_id -> Message

        # Consumer groups
        self.consumers: Dict[str, List[str]] = defaultdict(list)  # topic -> [consumer_ids]

        # Redis connection
        self._redis: Optional[aioredis.Redis] = None
        self._running = False
        self._redelivery_task: Optional[asyncio.Task] = None

    def _init_hash_ring(self):
        """Initialize the hash ring with this node and all peers."""
        self.hash_ring.add_node(self.node_url)
        for peer in self.peers:
            self.hash_ring.add_node(peer)

    async def start(self):
        """Start the queue system."""
        self._running = True
        try:
            self._redis = aioredis.from_url(self.redis_url, decode_responses=True)
            await self._load_persisted_messages()
        except Exception as e:
            logger.warning(f"[{self.node_id}] Redis not available for queue: {e}")
            self._redis = None

        self._redelivery_task = asyncio.create_task(self._redelivery_loop())
        logger.info(f"[{self.node_id}] Queue System started")

    async def stop(self):
        """Stop the queue system."""
        self._running = False
        if self._redelivery_task:
            self._redelivery_task.cancel()
        if self._redis:
            await self._redis.close()
        logger.info(f"[{self.node_id}] Queue System stopped")

    # --- Message Operations ---

    async def push_message(
        self,
        topic: str,
        data: Any,
        producer_id: str = "anonymous",
        priority: int = 0,
    ) -> Dict[str, Any]:
        """
        Push a message to the queue.
        Routes to the correct node based on consistent hashing.
        """
        message = Message(
            topic=topic,
            data=data,
            producer_id=producer_id,
            priority=priority,
        )

        # Determine which node owns this topic
        target_node = self.hash_ring.get_node(topic)

        if target_node == self.node_url:
            # This node owns the topic
            return await self._store_message(message)
        else:
            # Forward to the owning node
            if self.node_client:
                logger.info(f"[{self.node_id}] Forwarding PUSH for topic '{topic}' to owner {target_node}")
                response = await self.node_client.send_to_peer(
                    target_node,
                    "/queue/push",
                    {
                        "topic": topic,
                        "data": data,
                        "producer_id": producer_id,
                        "priority": priority,
                    },
                )
                if response:
                    return response

            # Fallback: store locally if forwarding fails
            logger.warning(
                f"[{self.node_id}] Failed to forward to {target_node}, storing locally"
            )
            return await self._store_message(message)

    async def _store_message(self, message: Message) -> Dict[str, Any]:
        """Store a message in the local queue and persist to Redis."""
        self.queues[message.topic].append(message)

        # Persist to Redis
        if self._redis:
            try:
                key = f"queue:{self.node_id}:{message.topic}"
                await self._redis.rpush(key, json.dumps(message.to_dict()))
            except Exception as e:
                logger.error(f"[{self.node_id}] Failed to persist message: {e}")

        if self.metrics:
            self.metrics.queue_metrics["messages_pushed"] += 1

        logger.info(
            f"[{self.node_id}] Message stored: {message.id} on topic '{message.topic}'"
        )
        return {
            "status": "queued",
            "message_id": message.id,
            "topic": message.topic,
            "node": self.node_id,
            "queue_depth": len(self.queues[message.topic]),
        }

    async def consume_message(
        self,
        topic: str,
        consumer_id: str = "anonymous",
    ) -> Dict[str, Any]:
        """
        Consume a message from a topic.
        Returns the next available message (at-least-once delivery).
        """
        # Determine which node owns this topic
        target_node = self.hash_ring.get_node(topic)

        if target_node != self.node_url:
            # Forward to owning node
            if self.node_client:
                logger.info(f"[{self.node_id}] Forwarding CONSUME for topic '{topic}' to owner {target_node}")
                response = await self.node_client.send_to_peer(
                    target_node,
                    "/queue/consume",
                    {"topic": topic, "consumer_id": consumer_id},
                )
                if response:
                    return response
            return {"status": "error", "message": f"Topic owner {target_node} unavailable"}

        # Register consumer
        if consumer_id not in self.consumers[topic]:
            self.consumers[topic].append(consumer_id)

        # Get next message
        queue = self.queues.get(topic, [])
        if not queue:
            return {
                "status": "empty",
                "topic": topic,
                "message": "No messages available",
            }

        # Pop the first message and move to pending acks
        message = queue.pop(0)
        message.delivery_count += 1
        message.timestamp = time.time()  # Update timestamp to delivery time
        self.pending_acks[message.id] = message

        if self.metrics:
            self.metrics.queue_metrics["messages_consumed"] += 1

        logger.info(
            f"[{self.node_id}] Message consumed: {message.id} by {consumer_id}"
        )
        return {
            "status": "delivered",
            "message_id": message.id,
            "topic": topic,
            "data": message.data,
            "delivery_count": message.delivery_count,
            "consumer_id": consumer_id,
        }

    async def ack_message(
        self,
        message_id: str,
        consumer_id: str = "anonymous",
        topic: str = "",
    ) -> Dict[str, Any]:
        """Acknowledge a consumed message (completes delivery)."""
        # If topic is provided, route to the owning node
        if topic:
            target_node = self.hash_ring.get_node(topic)
            if target_node and target_node != self.node_url:
                if self.node_client:
                    logger.info(f"[{self.node_id}] Forwarding ACK for message {message_id} (topic: {topic}) to owner {target_node}")
                    response = await self.node_client.send_to_peer(
                        target_node,
                        "/queue/ack",
                        {
                            "message_id": message_id,
                            "consumer_id": consumer_id,
                            "topic": topic,
                        },
                    )
                    if response:
                        return response

        logger.info(f"[{self.node_id}] Processing ACK locally for message {message_id}")
        if message_id not in self.pending_acks:
            logger.warning(f"[{self.node_id}] ACK FAILED: Message {message_id} not in pending_acks. Current keys: {list(self.pending_acks.keys())}")
            return {
                "status": "error",
                "message": f"Message {message_id} not found in pending acks",
            }

        message = self.pending_acks.pop(message_id)
        message.acked = True

        # Remove from Redis persistence
        if self._redis:
            try:
                key = f"queue:{self.node_id}:{message.topic}"
                await self._redis.lrem(key, 1, json.dumps(message.to_dict()))
            except Exception:
                pass  # Best effort

        if self.metrics:
            self.metrics.queue_metrics["messages_acked"] += 1

        logger.info(f"[{self.node_id}] Message acked: {message_id} by {consumer_id}")
        return {
            "status": "acked",
            "message_id": message_id,
            "topic": message.topic,
        }

    # --- Recovery & Redelivery ---

    async def _redelivery_loop(self):
        """Re-deliver unacked messages after timeout."""
        while self._running:
            try:
                now = time.time()
                expired = []
                for msg_id, message in list(self.pending_acks.items()):
                    if (now - message.timestamp) > self.ack_timeout:
                        expired.append(msg_id)

                for msg_id in expired:
                    message = self.pending_acks.pop(msg_id)
                    message.timestamp = time.time()
                    self.queues[message.topic].insert(0, message)  # Re-queue at front

                    if self.metrics:
                        self.metrics.queue_metrics["messages_redelivered"] += 1

                    logger.info(
                        f"[{self.node_id}] Message redelivered: {msg_id} "
                        f"(delivery #{message.delivery_count})"
                    )

                await asyncio.sleep(5.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.node_id}] Redelivery error: {e}")
                await asyncio.sleep(5.0)

    async def _load_persisted_messages(self):
        """Load persisted messages from Redis on startup."""
        if not self._redis:
            return
        try:
            # Scan for queue keys belonging to this node
            async for key in self._redis.scan_iter(f"queue:{self.node_id}:*"):
                topic = key.split(":", 2)[2]
                messages = await self._redis.lrange(key, 0, -1)
                for msg_data in messages:
                    msg = Message.from_dict(json.loads(msg_data))
                    self.queues[topic].append(msg)
                if messages:
                    logger.info(
                        f"[{self.node_id}] Recovered {len(messages)} messages for topic '{topic}'"
                    )
        except Exception as e:
            logger.error(f"[{self.node_id}] Failed to load messages: {e}")

    def handle_node_failure(self, failed_node: str):
        """Handle a node failure by rehashing the ring."""
        logger.info(f"[{self.node_id}] Handling failure of {failed_node}")
        self.hash_ring.remove_node(failed_node)

    def handle_node_recovery(self, recovered_node: str):
        """Handle a node recovery by adding it back to the ring."""
        logger.info(f"[{self.node_id}] Node recovered: {recovered_node}")
        self.hash_ring.add_node(recovered_node)

    # --- Status ---

    def get_status(self) -> Dict[str, Any]:
        """Get queue system status."""
        topic_stats = {}
        for topic, messages in self.queues.items():
            target = self.hash_ring.get_node(topic)
            topic_stats[topic] = {
                "depth": len(messages),
                "owner_node": target,
                "consumers": self.consumers.get(topic, []),
            }

        return {
            "node_id": self.node_id,
            "topics": topic_stats,
            "total_messages": sum(len(m) for m in self.queues.values()),
            "pending_acks": len(self.pending_acks),
            "hash_ring": self.hash_ring.get_ring_info(),
        }
