"""
Benchmark runner for the distributed synchronization system.

Runs performance tests against a running cluster and generates
JSON results + matplotlib visualizations.

Usage:
    python benchmarks/benchmark_runner.py [--base-url http://localhost:8001]
"""

import asyncio
import time
import json
import os
import sys
import statistics
from typing import Dict, List, Any

import httpx

# Configuration
BASE_URLS = [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003",
]
API_KEY = "sync-api-key-2026"
RESULTS_DIR = "benchmarks/results"
GRAPHS_DIR = "benchmarks/graphs"


async def benchmark_lock_contention(client: httpx.AsyncClient, num_requests: int = 50) -> Dict:
    """Benchmark lock acquisition and release under contention."""
    print(f"\n📊 Lock Contention Benchmark ({num_requests} requests)...")
    latencies = []
    errors = 0

    for i in range(num_requests):
        url = BASE_URLS[i % len(BASE_URLS)]
        start = time.time()
        try:
            # Acquire lock
            resp = await client.post(f"{url}/lock/acquire", json={
                "resource": f"resource_{i % 5}",
                "client_id": f"client_{i}",
                "lock_type": "exclusive",
                "ttl": 5.0,
                "timeout": 3.0,
            })
            latency = (time.time() - start) * 1000
            latencies.append(latency)

            # Release lock
            if resp.status_code == 200 and resp.json().get("status") == "granted":
                await client.post(f"{url}/lock/release", json={
                    "resource": f"resource_{i % 5}",
                    "client_id": f"client_{i}",
                })
        except Exception as e:
            errors += 1
            latencies.append((time.time() - start) * 1000)

    return _calc_stats("Lock Contention", latencies, errors)


async def benchmark_queue_throughput(client: httpx.AsyncClient, num_messages: int = 100) -> Dict:
    """Benchmark queue push and consume throughput."""
    print(f"\n📊 Queue Throughput Benchmark ({num_messages} messages)...")

    # Push messages
    push_latencies = []
    push_start = time.time()
    for i in range(num_messages):
        url = BASE_URLS[i % len(BASE_URLS)]
        start = time.time()
        try:
            await client.post(f"{url}/queue/push", json={
                "topic": f"topic_{i % 3}",
                "data": {"msg_id": i, "payload": f"test_data_{i}"},
                "producer_id": f"producer_{i % 3}",
            })
            push_latencies.append((time.time() - start) * 1000)
        except Exception:
            push_latencies.append((time.time() - start) * 1000)

    push_total = time.time() - push_start
    push_throughput = num_messages / push_total if push_total > 0 else 0

    # Consume messages
    consume_latencies = []
    consumed = 0
    consume_start = time.time()
    for i in range(num_messages):
        url = BASE_URLS[i % len(BASE_URLS)]
        start = time.time()
        try:
            resp = await client.post(f"{url}/queue/consume", json={
                "topic": f"topic_{i % 3}",
                "consumer_id": f"consumer_{i % 2}",
            })
            consume_latencies.append((time.time() - start) * 1000)
            if resp.status_code == 200 and resp.json().get("status") == "delivered":
                consumed += 1
                # Ack
                msg_id = resp.json().get("message_id", "")
                await client.post(f"{url}/queue/ack", json={
                    "message_id": msg_id,
                    "consumer_id": f"consumer_{i % 2}",
                })
        except Exception:
            consume_latencies.append((time.time() - start) * 1000)

    consume_total = time.time() - consume_start

    return {
        "test": "Queue Throughput",
        "push": _calc_stats("Push", push_latencies, 0),
        "consume": _calc_stats("Consume", consume_latencies, 0),
        "push_throughput_msg_per_sec": round(push_throughput, 2),
        "consume_throughput_msg_per_sec": round(consumed / consume_total, 2) if consume_total > 0 else 0,
        "messages_pushed": num_messages,
        "messages_consumed": consumed,
    }


async def benchmark_cache_performance(client: httpx.AsyncClient, num_ops: int = 100) -> Dict:
    """Benchmark cache read/write with MESI state transitions."""
    print(f"\n📊 Cache Performance Benchmark ({num_ops} operations)...")

    write_latencies = []
    read_latencies = []
    hits = 0
    misses = 0

    # Write phase
    for i in range(num_ops // 2):
        url = BASE_URLS[i % len(BASE_URLS)]
        start = time.time()
        try:
            await client.put(f"{url}/cache/key_{i % 20}", json={"value": f"value_{i}"})
            write_latencies.append((time.time() - start) * 1000)
        except Exception:
            write_latencies.append((time.time() - start) * 1000)

    # Read phase (mix of hits and misses)
    for i in range(num_ops // 2):
        url = BASE_URLS[i % len(BASE_URLS)]
        key = f"key_{i % 25}"  # Some will be misses (20-24)
        start = time.time()
        try:
            resp = await client.get(f"{url}/cache/{key}")
            read_latencies.append((time.time() - start) * 1000)
            if resp.status_code == 200:
                status = resp.json().get("status", "")
                if status in ("hit", "miss_peer"):
                    hits += 1
                else:
                    misses += 1
        except Exception:
            read_latencies.append((time.time() - start) * 1000)
            misses += 1

    total = hits + misses
    hit_rate = round(hits / total * 100, 2) if total > 0 else 0

    return {
        "test": "Cache Performance",
        "write": _calc_stats("Write", write_latencies, 0),
        "read": _calc_stats("Read", read_latencies, 0),
        "hit_rate_pct": hit_rate,
        "hits": hits,
        "misses": misses,
    }


async def benchmark_failure_recovery(client: httpx.AsyncClient) -> Dict:
    """Benchmark leader election and failure detection timing."""
    print("\n📊 Failure Recovery Benchmark...")

    results = {}

    # Check current leader
    try:
        resp = await client.get(f"{BASE_URLS[0]}/raft/state")
        if resp.status_code == 200:
            results["initial_state"] = resp.json()
    except Exception:
        results["initial_state"] = "unavailable"

    # Check cluster health
    try:
        resp = await client.get(f"{BASE_URLS[0]}/cluster/status")
        if resp.status_code == 200:
            results["cluster_status"] = resp.json()
    except Exception:
        results["cluster_status"] = "unavailable"

    results["test"] = "Failure Recovery"
    results["note"] = "Full failure test requires manually stopping a node"

    return results


def _calc_stats(name: str, latencies: List[float], errors: int) -> Dict:
    """Calculate statistics from latency measurements."""
    if not latencies:
        return {"test": name, "error": "no data"}

    sorted_lat = sorted(latencies)
    p50_idx = int(len(sorted_lat) * 0.50)
    p95_idx = int(len(sorted_lat) * 0.95)
    p99_idx = int(len(sorted_lat) * 0.99)

    return {
        "test": name,
        "total_requests": len(latencies),
        "errors": errors,
        "avg_ms": round(statistics.mean(latencies), 2),
        "min_ms": round(sorted_lat[0], 2),
        "max_ms": round(sorted_lat[-1], 2),
        "p50_ms": round(sorted_lat[p50_idx], 2),
        "p95_ms": round(sorted_lat[p95_idx], 2),
        "p99_ms": round(sorted_lat[p99_idx], 2),
        "std_dev_ms": round(statistics.stdev(latencies), 2) if len(latencies) > 1 else 0,
    }


async def run_all_benchmarks():
    """Run all benchmarks and save results."""
    os.makedirs(RESULTS_DIR, exist_ok=True)

    headers = {"X-API-Key": API_KEY}
    async with httpx.AsyncClient(timeout=30.0, headers=headers) as client:
        # Verify cluster is running
        print("🔍 Checking cluster health...")
        try:
            resp = await client.get(f"{BASE_URLS[0]}/health")
            if resp.status_code != 200:
                print("❌ Cluster not responding. Start it first.")
                return
            print(f"✅ Cluster healthy: {resp.json()}")
        except Exception as e:
            print(f"❌ Cannot connect to cluster: {e}")
            print("   Start the cluster first: docker-compose -f docker/docker-compose.yml up -d")
            return

        # Run benchmarks
        results = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "cluster_size": len(BASE_URLS),
        }

        results["lock_contention"] = await benchmark_lock_contention(client)
        results["queue_throughput"] = await benchmark_queue_throughput(client)
        results["cache_performance"] = await benchmark_cache_performance(client)
        results["failure_recovery"] = await benchmark_failure_recovery(client)

        # Save results
        results_path = os.path.join(RESULTS_DIR, "benchmark_results.json")
        with open(results_path, "w") as f:
            json.dump(results, f, indent=2)

        print(f"\n✅ Results saved to {results_path}")

        # Print summary
        print("\n" + "=" * 60)
        print("📊 BENCHMARK SUMMARY")
        print("=" * 60)

        lock = results.get("lock_contention", {})
        print(f"\n🔒 Lock Contention:")
        print(f"   Avg: {lock.get('avg_ms', 'N/A')}ms | P99: {lock.get('p99_ms', 'N/A')}ms")

        queue = results.get("queue_throughput", {})
        print(f"\n📨 Queue Throughput:")
        print(f"   Push: {queue.get('push_throughput_msg_per_sec', 'N/A')} msg/s")
        print(f"   Consume: {queue.get('consume_throughput_msg_per_sec', 'N/A')} msg/s")

        cache = results.get("cache_performance", {})
        print(f"\n💾 Cache Performance:")
        print(f"   Hit Rate: {cache.get('hit_rate_pct', 'N/A')}%")

        return results


if __name__ == "__main__":
    results = asyncio.run(run_all_benchmarks())
    if results:
        # Generate visualizations
        try:
            from benchmarks.visualize import generate_all_charts
            generate_all_charts(results)
        except ImportError:
            print("\n⚠️  Run visualize.py separately to generate charts")
