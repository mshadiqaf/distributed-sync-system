"""
Visualization generator for benchmark results.
Creates charts using matplotlib.
"""

import json
import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

RESULTS_DIR = "benchmarks/results"
GRAPHS_DIR = "benchmarks/graphs"


def generate_all_charts(results=None):
    os.makedirs(GRAPHS_DIR, exist_ok=True)
    if results is None:
        path = os.path.join(RESULTS_DIR, "benchmark_results.json")
        if not os.path.exists(path):
            print("No results found.")
            return
        with open(path) as f:
            results = json.load(f)

    _latency_chart(results)
    _throughput_chart(results)
    _cache_chart(results)
    _dashboard(results)
    print(f"Charts saved to {GRAPHS_DIR}/")


def _latency_chart(r):
    fig, ax = plt.subplots(figsize=(10, 6))
    tests, avgs, p99s = [], [], []
    for name, data in [("Lock", r.get("lock_contention", {})),
                       ("Push", r.get("queue_throughput", {}).get("push", {})),
                       ("Consume", r.get("queue_throughput", {}).get("consume", {})),
                       ("Cache Write", r.get("cache_performance", {}).get("write", {})),
                       ("Cache Read", r.get("cache_performance", {}).get("read", {}))]:
        if "avg_ms" in data:
            tests.append(name)
            avgs.append(data["avg_ms"])
            p99s.append(data["p99_ms"])
    if not tests:
        plt.close()
        return
    x = range(len(tests))
    w = 0.35
    ax.bar([i - w/2 for i in x], avgs, w, label='Avg', color='#4CAF50', alpha=0.8)
    ax.bar([i + w/2 for i in x], p99s, w, label='P99', color='#FF5722', alpha=0.8)
    ax.set_ylabel('Latency (ms)')
    ax.set_title('Operation Latency Comparison', fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(tests)
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(GRAPHS_DIR, "latency_comparison.png"), dpi=150)
    plt.close()


def _throughput_chart(r):
    fig, ax = plt.subplots(figsize=(8, 5))
    q = r.get("queue_throughput", {})
    vals = [q.get("push_throughput_msg_per_sec", 0), q.get("consume_throughput_msg_per_sec", 0)]
    bars = ax.bar(["Push", "Consume"], vals, color=["#2196F3", "#FF9800"], alpha=0.8)
    ax.set_ylabel('Messages/sec')
    ax.set_title('Queue Throughput', fontweight='bold')
    ax.grid(axis='y', alpha=0.3)
    for b, v in zip(bars, vals):
        ax.annotate(f'{v:.1f}', xy=(b.get_x() + b.get_width()/2, b.get_height()),
                    xytext=(0, 5), textcoords="offset points", ha='center', fontweight='bold')
    plt.tight_layout()
    plt.savefig(os.path.join(GRAPHS_DIR, "queue_throughput.png"), dpi=150)
    plt.close()


def _cache_chart(r):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    c = r.get("cache_performance", {})
    hits, misses = c.get("hits", 0), c.get("misses", 0)
    if hits + misses > 0:
        ax1.pie([hits, misses], labels=[f'Hits ({hits})', f'Misses ({misses})'],
                colors=['#4CAF50', '#F44336'], autopct='%1.1f%%', startangle=90)
    ax1.set_title('Cache Hit Rate', fontweight='bold')
    w, rd = c.get("write", {}), c.get("read", {})
    if "avg_ms" in w and "avg_ms" in rd:
        ax2.bar(["Write", "Read"], [w["avg_ms"], rd["avg_ms"]], color=["#FF5722", "#2196F3"], alpha=0.8)
        ax2.set_ylabel('Latency (ms)')
        ax2.set_title('Cache Latency', fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(GRAPHS_DIR, "cache_performance.png"), dpi=150)
    plt.close()


def _dashboard(r):
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('Performance Dashboard', fontsize=16, fontweight='bold')
    lock = r.get("lock_contention", {})
    q = r.get("queue_throughput", {})
    c = r.get("cache_performance", {})

    ax = axes[0][0]
    metrics = {"Lock": lock.get("avg_ms", 0), "Push": q.get("push", {}).get("avg_ms", 0),
               "Consume": q.get("consume", {}).get("avg_ms", 0),
               "Cache W": c.get("write", {}).get("avg_ms", 0), "Cache R": c.get("read", {}).get("avg_ms", 0)}
    ax.barh(list(metrics.keys()), list(metrics.values()), color=['#4CAF50','#2196F3','#FF9800','#FF5722','#9C27B0'])
    ax.set_xlabel('Avg Latency (ms)')
    ax.set_title('Latency Overview')

    ax = axes[0][1]
    ax.bar(["Push", "Consume"], [q.get("push_throughput_msg_per_sec", 0), q.get("consume_throughput_msg_per_sec", 0)],
           color=["#2196F3", "#FF9800"])
    ax.set_ylabel("msg/sec")
    ax.set_title("Throughput")

    ax = axes[1][0]
    h, m = c.get("hits", 0), c.get("misses", 0)
    if h + m > 0:
        ax.pie([h, m], labels=['Hits', 'Misses'], colors=['#4CAF50', '#F44336'], autopct='%1.1f%%')
    ax.set_title("Cache Hit Rate")

    ax = axes[1][1]
    ax.axis('off')
    txt = f"Lock Avg: {lock.get('avg_ms','N/A')}ms\nPush: {q.get('push_throughput_msg_per_sec',0):.1f} msg/s\nHit Rate: {c.get('hit_rate_pct','N/A')}%\nCluster: {r.get('cluster_size',3)} nodes"
    ax.text(0.1, 0.5, txt, transform=ax.transAxes, fontsize=13, verticalalignment='center', fontfamily='monospace')
    ax.set_title("Summary")

    plt.tight_layout()
    plt.savefig(os.path.join(GRAPHS_DIR, "dashboard.png"), dpi=150)
    plt.close()


if __name__ == "__main__":
    generate_all_charts()
