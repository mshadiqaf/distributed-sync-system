"""Unit test untuk Distributed Queue dan ConsistentHashRing."""

import pytest
from src.nodes.queue_node import ConsistentHashRing, DistributedQueue, Message
from src.utils.metrics import MetricsCollector


class TestConsistentHashRing:
    def test_add_single_node(self):
        ring = ConsistentHashRing(virtual_nodes=10)
        ring.add_node("http://n1:8001")
        assert len(ring.nodes) == 1
        assert len(ring.ring) == 10

    def test_add_duplicate_node_ignored(self):
        ring = ConsistentHashRing(virtual_nodes=10)
        ring.add_node("http://n1:8001")
        ring.add_node("http://n1:8001")
        assert len(ring.nodes) == 1
        assert len(ring.ring) == 10

    def test_remove_node(self):
        ring = ConsistentHashRing(virtual_nodes=10)
        ring.add_node("http://n1:8001")
        ring.add_node("http://n2:8002")
        ring.remove_node("http://n1:8001")
        assert len(ring.nodes) == 1
        assert all(n == "http://n2:8002" for _, n in ring.ring)

    def test_get_node_single(self):
        ring = ConsistentHashRing(virtual_nodes=10)
        ring.add_node("http://n1:8001")
        assert ring.get_node("any_key") == "http://n1:8001"

    def test_get_node_after_remove(self):
        ring = ConsistentHashRing(virtual_nodes=10)
        ring.add_node("http://n1:8001")
        ring.add_node("http://n2:8002")
        ring.remove_node("http://n1:8001")
        assert ring.get_node("key") == "http://n2:8002"

    def test_get_node_empty_ring(self):
        ring = ConsistentHashRing(virtual_nodes=10)
        assert ring.get_node("key") is None

    def test_distribution_three_nodes(self):
        ring = ConsistentHashRing(virtual_nodes=150)
        for i in range(1, 4):
            ring.add_node(f"http://n{i}:800{i}")
        counts = {n: 0 for n in ring.nodes}
        for i in range(300):
            node = ring.get_node(f"topic_{i}")
            counts[node] += 1
        for count in counts.values():
            assert 30 < count < 200, f"Distribusi tidak merata: {counts}"

    def test_same_key_same_node(self):
        ring = ConsistentHashRing(virtual_nodes=50)
        ring.add_node("http://n1:8001")
        ring.add_node("http://n2:8002")
        node_first = ring.get_node("stable_topic")
        node_second = ring.get_node("stable_topic")
        assert node_first == node_second

    def test_ring_info_structure(self):
        ring = ConsistentHashRing(virtual_nodes=10)
        ring.add_node("http://n1:8001")
        info = ring.get_ring_info()
        assert "total_vnodes" in info
        assert "nodes" in info
        assert "distribution" in info


class TestMessage:
    def test_message_default_id_generated(self):
        m = Message(topic="t", data="d")
        assert m.id is not None
        assert len(m.id) > 0

    def test_message_to_dict(self):
        m = Message(topic="orders", data={"id": 1})
        d = m.to_dict()
        assert d["topic"] == "orders"
        assert d["data"] == {"id": 1}

    def test_message_from_dict(self):
        m = Message(topic="orders", data={"id": 1}, producer_id="shop")
        d = m.to_dict()
        m2 = Message.from_dict(d)
        assert m2.topic == "orders"
        assert m2.producer_id == "shop"


class TestDistributedQueue:
    @pytest.fixture
    def queue(self):
        metrics = MetricsCollector("test")
        return DistributedQueue(
            node_id="node1",
            node_url="http://node1:8001",
            peers=[],
            redis_url="redis://localhost:6379/0",
            virtual_nodes=10,
            ack_timeout=5.0,
            metrics=metrics,
        )

    @pytest.mark.asyncio
    async def test_push_local_topic(self, queue):
        r = await queue.push_message("test-topic", {"x": 1}, "prod")
        assert r["status"] == "queued"
        assert r["message_id"] is not None

    @pytest.mark.asyncio
    async def test_consume_after_push(self, queue):
        await queue.push_message("t1", "hello", "prod")
        r = await queue.consume_message("t1", "consumer")
        assert r["status"] == "delivered"
        assert r["data"] == "hello"

    @pytest.mark.asyncio
    async def test_consume_empty(self, queue):
        r = await queue.consume_message("empty-topic", "consumer")
        assert r["status"] == "empty"

    @pytest.mark.asyncio
    async def test_ack_removes_from_pending(self, queue):
        await queue.push_message("t1", "data", "prod")
        consume = await queue.consume_message("t1", "c1")
        mid = consume["message_id"]
        assert mid in queue.pending_acks
        ack = await queue.ack_message(mid, "c1")
        assert ack["status"] == "acked"
        assert mid not in queue.pending_acks

    @pytest.mark.asyncio
    async def test_ack_nonexistent_message(self, queue):
        r = await queue.ack_message("nonexistent-id", "c1")
        assert r["status"] == "error"

    @pytest.mark.asyncio
    async def test_fifo_order(self, queue):
        for i in range(3):
            await queue.push_message("ordered", f"msg-{i}", "prod")
        results = []
        for _ in range(3):
            r = await queue.consume_message("ordered", "c1")
            results.append(r["data"])
        assert results == ["msg-0", "msg-1", "msg-2"]

    @pytest.mark.asyncio
    async def test_metrics_updated(self, queue):
        await queue.push_message("t", "d", "p")
        await queue.consume_message("t", "c")
        assert queue.metrics.queue_metrics["messages_pushed"] == 1
        assert queue.metrics.queue_metrics["messages_consumed"] == 1

    def test_get_status_structure(self, queue):
        status = queue.get_status()
        assert "node_id" in status
        assert "total_messages" in status
        assert "pending_acks" in status
        assert "hash_ring" in status
