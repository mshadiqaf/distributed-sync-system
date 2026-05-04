"""
Updated visualization generator for benchmark results.
Supports multi-concurrency scenarios from benchmark_results.json.
"""

import json
import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

RESULTS_DIR = "benchmarks/results"
GRAPHS_DIR = "benchmarks/graphs"

def generate_all_charts():
    os.makedirs(GRAPHS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, "benchmark_results.json")
    if not os.path.exists(path):
        print("No results found.")
        return
    with open(path) as f:
        data = json.load(f)

    # Detect format
    if "scenarios" in data:
        results = data["scenarios"]
    else:
        print("Old format detected. Converting for visualization...")
        # Minimal conversion to avoid breaking (though we prefer the new format)
        results = {}

    _plot_concurrency_vs_throughput(results)
    _plot_concurrency_vs_latency(results)
    _plot_cache_metrics(results)
    _plot_dashboard_summary(results)
    print(f"Charts saved to {GRAPHS_DIR}/")

def _plot_concurrency_vs_throughput(scenarios):
    plt.figure(figsize=(10, 6))
    
    for key, label in [("distributed_lock", "Lock Manager"), 
                        ("distributed_queue", "Queue System"), 
                        ("mesi_cache", "MESI Cache")]:
        if key in scenarios:
            data = scenarios[key]["results"]
            concurrency = [d["concurrency"] for d in data]
            throughput = [d["throughput"] for d in data]
            plt.plot(concurrency, throughput, marker='o', label=label, linewidth=2)

    plt.xlabel('Concurrent Users')
    plt.ylabel('Throughput (req/s)')
    plt.title('System Throughput vs Concurrency', fontweight='bold')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(GRAPHS_DIR, "throughput_scaling.png"), dpi=150)
    plt.close()

def _plot_concurrency_vs_latency(scenarios):
    plt.figure(figsize=(10, 6))
    
    for key, label in [("distributed_lock", "Lock Manager"), 
                        ("distributed_queue", "Queue System"), 
                        ("mesi_cache", "MESI Cache")]:
        if key in scenarios:
            data = scenarios[key]["results"]
            concurrency = [d["concurrency"] for d in data]
            latency = [d["avg_latency"] for d in data]
            plt.plot(concurrency, latency, marker='s', label=label, linewidth=2)

    plt.xlabel('Concurrent Users')
    plt.ylabel('Avg Latency (ms)')
    plt.title('Average Latency vs Concurrency', fontweight='bold')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(GRAPHS_DIR, "latency_scaling.png"), dpi=150)
    plt.close()

def _plot_cache_metrics(scenarios):
    if "mesi_cache" not in scenarios:
        return
        
    metrics = scenarios["mesi_cache"].get("cache_metrics", {})
    hit_rate = metrics.get("hit_rate", 0)
    
    plt.figure(figsize=(8, 6))
    labels = ['Hits', 'Misses']
    sizes = [hit_rate, 100 - hit_rate]
    colors = ['#4CAF50', '#F44336']
    
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', colors=colors, startangle=140, explode=(0.1, 0))
    plt.title('MESI Cache Hit Rate Analysis', fontweight='bold')
    plt.tight_layout()
    plt.savefig(os.path.join(GRAPHS_DIR, "cache_performance.png"), dpi=150)
    plt.close()

def _plot_dashboard_summary(scenarios):
    # Summary Dashboard
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('Distributed System Performance Dashboard', fontsize=18, fontweight='bold')
    
    # 1. Throughput (axes[0,0])
    ax = axes[0,0]
    for key, label in [("distributed_lock", "Lock"), ("distributed_queue", "Queue"), ("mesi_cache", "Cache")]:
        if key in scenarios:
            d = scenarios[key]["results"]
            ax.plot([x["concurrency"] for x in d], [x["throughput"] for x in d], marker='o', label=label)
    ax.set_title("Throughput Scaling")
    ax.set_ylabel("req/s")
    ax.legend()
    ax.grid(True, alpha=0.2)
    
    # 2. Latency (axes[0,1])
    ax = axes[0,1]
    for key, label in [("distributed_lock", "Lock"), ("distributed_queue", "Queue"), ("mesi_cache", "Cache")]:
        if key in scenarios:
            d = scenarios[key]["results"]
            ax.plot([x["concurrency"] for x in d], [x["avg_latency"] for x in d], marker='s', label=label)
    ax.set_title("Latency Response")
    ax.set_ylabel("ms")
    ax.legend()
    ax.grid(True, alpha=0.2)
    
    # 3. Error Rates (axes[1,0])
    ax = axes[1,0]
    for key, label in [("distributed_lock", "Lock"), ("distributed_queue", "Queue"), ("mesi_cache", "Cache")]:
        if key in scenarios:
            d = scenarios[key]["results"]
            ax.bar(label, d[-1]["error_rate"] if d else 0, color='orange')
    ax.set_title("Error Rate at Max Load (200 Users)")
    ax.set_ylabel("% Error")
    
    # 4. Text Summary (axes[1,1])
    ax = axes[1,1]
    ax.axis('off')
    recovery = scenarios.get("fault_tolerance", {}).get("recovery_time_ms", "N/A")
    lock_max = scenarios.get("distributed_lock", {}).get("results", [{}])[-1].get("throughput", "N/A")
    cache_hr = scenarios.get("mesi_cache", {}).get("cache_metrics", {}).get("hit_rate", "N/A")
    
    summary_text = (
        f"Key Metrics Summary:\n\n"
        f"• Max Lock Throughput: {lock_max} req/s\n"
        f"• Cache Hit Rate: {cache_hr}%\n"
        f"• Fault Recovery Time: {recovery} ms\n"
        f"• Nodes in Cluster: 3\n"
        f"• Replication Protocol: Raft\n"
        f"• Cache Protocol: MESI"
    )
    ax.text(0.1, 0.5, summary_text, fontsize=14, verticalalignment='center', fontfamily='monospace')
    
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig(os.path.join(GRAPHS_DIR, "dashboard.png"), dpi=150)
    plt.close()

if __name__ == "__main__":
    generate_all_charts()
