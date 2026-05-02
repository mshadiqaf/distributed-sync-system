# 🔄 Distributed Synchronization System

A distributed synchronization system implementing core distributed systems concepts including **Raft consensus**, **distributed locking**, **consistent-hashing message queuing**, and **MESI cache coherence**.

Built as a university project for Distributed Systems (Sistem Terdistribusi) — Tugas 3.

---

## 🏗️ Architecture Overview

```
┌────────────────────────────────────────────────────────┐
│                    Docker Network                      │
│                                                        │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐         │
│  │  Node 1  │◄──►│  Node 2  │◄──►│  Node 3  │         │
│  │ :8001    │    │ :8002    │    │ :8003    │         │
│  │          │    │          │    │          │         │
│  │ ┌──────┐ │    │ ┌──────┐ │    │ ┌──────┐ │         │
│  │ │ Raft │ │    │ │ Raft │ │    │ │ Raft │ │         │
│  │ │Leader│ │    │ │Follow│ │    │ │Follow│ │         │
│  │ ├──────┤ │    │ ├──────┤ │    │ ├──────┤ │         │
│  │ │ Lock │ │    │ │ Lock │ │    │ │ Lock │ │         │
│  │ │ Mgr  │ │    │ │ Mgr  │ │    │ │ Mgr  │ │         │
│  │ ├──────┤ │    │ ├──────┤ │    │ ├──────┤ │         │
│  │ │Queue │ │    │ │Queue │ │    │ │Queue │ │         │
│  │ ├──────┤ │    │ ├──────┤ │    │ ├──────┤ │         │
│  │ │MESI  │ │    │ │MESI  │ │    │ │MESI  │ │         │
│  │ │Cache │ │    │ │Cache │ │    │ │Cache │ │         │
│  │ └──────┘ │    │ └──────┘ │    │ └──────┘ │         │
│  └──────────┘    └──────────┘    └──────────┘         │
│        │               │               │               │
│        └───────────────┼───────────────┘               │
│                        │                               │
│                 ┌──────┴──────┐                        │
│                 │    Redis    │                        │
│                 │   :6379     │                        │
│                 └─────────────┘                        │
└────────────────────────────────────────────────────────┘
```

Each node runs the **same Docker image** with different configuration, forming a 3-node cluster orchestrated by Docker Compose.

---

## 🚀 Features

### 1. Distributed Lock Manager (Raft Consensus)
- **Leader election** with randomized timeouts
- **Shared (read)** and **Exclusive (write)** locks
- **Deadlock detection** via wait-for graph cycle analysis (DFS)
- **TTL-based auto-release** for stale locks
- Lock operations replicated through Raft log

### 2. Distributed Queue (Consistent Hashing)
- **Consistent hash ring** with virtual nodes (150 vnodes per node)
- **Topic-based routing** — messages routed to the node responsible for a topic
- **At-least-once delivery** with ack-based tracking
- **Automatic redelivery** for unacked messages after timeout
- Redis-backed persistence for message durability

### 3. Cache Coherence (MESI Protocol)
- Full **4-state MESI** state machine (Modified, Exclusive, Shared, Invalid)
- **Snoop-based bus protocol** via HTTP for inter-node communication
- **Write-back policy** — Modified entries flushed to Redis on eviction
- **LRU replacement** with configurable cache size
- State transition logging for debugging and visualization

### 4. Security (Bonus)
- **API key authentication** with role-based access control (RBAC)
- **3-tier roles**: Admin, Writer, Reader
- **Inter-node authentication** via shared secret
- **Audit logging** with request tracking

---

## 📦 Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.12+ |
| Framework | FastAPI (async) |
| State Store | Redis 7 |
| HTTP Client | httpx (async) |
| Container | Docker & Docker Compose |
| Visualization | matplotlib |
| Testing | pytest + pytest-asyncio |

---

## 🛠️ Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.12+ (for local development)
- Git

### Option 1: Docker (Recommended)

```bash
# Clone the repository
git clone https://github.com/mshadiqaf/distributed-sync-system.git
cd distributed-sync-system

# Start the 3-node cluster
docker-compose -f docker/docker-compose.yml up --build -d

# Verify all nodes are running
curl http://localhost:8001/health
curl http://localhost:8002/health
curl http://localhost:8003/health
```

### Option 2: Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Start Redis
docker run -d --name redis -p 6379:6379 redis:7-alpine

# Start nodes (in separate terminals)
NODE_ID=node1 NODE_PORT=8001 PEER_NODES=http://localhost:8002,http://localhost:8003 python -m uvicorn src.main:app --host 0.0.0.0 --port 8001
NODE_ID=node2 NODE_PORT=8002 PEER_NODES=http://localhost:8001,http://localhost:8003 python -m uvicorn src.main:app --host 0.0.0.0 --port 8002
NODE_ID=node3 NODE_PORT=8003 PEER_NODES=http://localhost:8001,http://localhost:8002 python -m uvicorn src.main:app --host 0.0.0.0 --port 8003
```

---

## 📡 API Endpoints

Access the interactive API documentation at: `http://localhost:8001/docs`

### System
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Node health check |
| GET | `/peers` | List peer nodes |
| GET | `/metrics` | Performance metrics |
| GET | `/cluster/status` | Cluster-wide health |

### Raft Consensus
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/raft/state` | Current Raft state (role, term, leader) |
| POST | `/raft/request-vote` | RequestVote RPC (internal) |
| POST | `/raft/append-entries` | AppendEntries RPC (internal) |

### Lock Manager
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/lock/acquire` | Acquire a distributed lock |
| POST | `/lock/release` | Release a lock |
| GET | `/lock/status` | All active locks |
| GET | `/lock/deadlocks` | Wait-for graph analysis |

### Queue System
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/queue/push` | Push message to topic |
| POST | `/queue/consume` | Consume from topic |
| POST | `/queue/ack` | Acknowledge message |
| GET | `/queue/status` | Queue depths and topics |
| GET | `/queue/ring` | Hash ring visualization |

### Cache (MESI)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/cache/{key}` | Read with MESI protocol |
| PUT | `/cache/{key}` | Write with MESI protocol |
| DELETE | `/cache/{key}` | Invalidate cache entry |
| GET | `/cache-stats` | MESI state distribution |
| GET | `/cache-entries` | All cache entries |

### Security
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/audit/logs` | Audit log entries |

---

## 🧪 Testing

### Unit & Integration Tests
```bash
# Install test dependencies
pip install pytest pytest-asyncio

# Run tests
python -m pytest tests/ -v
```

### Benchmarks
```bash
# Start the cluster first, then:
python benchmarks/benchmark_runner.py

# Generate visualization charts
python benchmarks/visualize.py
```

Benchmark results are saved to `benchmarks/results/` and charts to `benchmarks/graphs/`.

---

## 📁 Project Structure

```
distributed-sync-system/
├── src/
│   ├── main.py                     # Application entry point
│   ├── nodes/
│   │   ├── base_node.py            # FastAPI app factory + all endpoints
│   │   ├── lock_manager.py         # Distributed Lock Manager
│   │   ├── queue_node.py           # Distributed Queue + Consistent Hashing
│   │   └── cache_node.py           # MESI Cache Coherence
│   ├── consensus/
│   │   └── raft.py                 # Simplified Raft implementation
│   ├── communication/
│   │   ├── message_passing.py      # Async HTTP inter-node client
│   │   └── failure_detector.py     # Heartbeat-based failure detection
│   └── utils/
│       ├── config.py               # Pydantic-based configuration
│       ├── metrics.py              # In-memory metrics collector
│       └── security.py             # RBAC + API key + audit logging
├── docker/
│   ├── Dockerfile.node             # Single node Docker image
│   └── docker-compose.yml          # 3-node cluster orchestration
├── benchmarks/
│   ├── benchmark_runner.py         # Performance benchmark suite
│   └── visualize.py                # Matplotlib chart generator
├── tests/
│   └── test_integration.py         # Integration tests
├── requirements.txt                # Pinned Python dependencies
├── .env.example                    # Environment variable template
└── README.md                       # This file
```

---

## ⚙️ Configuration

All configuration is done via environment variables (see `.env.example`):

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ID` | `node1` | Unique node identifier |
| `NODE_PORT` | `8001` | HTTP server port |
| `PEER_NODES` | _(empty)_ | Comma-separated peer URLs |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection |
| `ELECTION_TIMEOUT_MIN` | `1500` | Min election timeout (ms) |
| `ELECTION_TIMEOUT_MAX` | `3000` | Max election timeout (ms) |
| `HEARTBEAT_INTERVAL` | `500` | Leader heartbeat interval (ms) |
| `CACHE_MAX_SIZE` | `1000` | Max cache entries per node |
| `API_KEY` | _(empty)_ | API key for writer role |
| `ADMIN_KEY` | _(empty)_ | API key for admin role |

---

## 📊 MESI State Transitions

```
       ┌─────────────────────────────────────────┐
       │              BusRd (Snoop)               │
       │         ┌───────────────────┐            │
       ▼         │                   ▼            │
   ┌───────┐  Read Hit         ┌───────┐         │
   │   I   │──────────────────►│   E   │         │
   │Invalid│  (from store,     │Exclus.│         │
   └───────┘   no other copy)  └───────┘         │
       ▲                            │             │
       │ BusRdX                     │ BusRd       │
       │ (Snoop)            Write   │ (Snoop)     │
       │                     │      ▼             │
   ┌───────┐                 │  ┌───────┐         │
   │   S   │◄────────────────┘  │   M   │         │
   │Shared │   BusRd (Snoop)    │Modif. │─────────┘
   └───────┘                    └───────┘
       │                            ▲
       │         Write              │
       └────────────────────────────┘
```

---

## 👤 Author

**Muhammad Shadiq Al Furqan**
Sistem Terdistribusi — 2026

## 📄 License

MIT
