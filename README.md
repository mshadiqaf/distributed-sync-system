# Distributed Synchronization System

Sistem sinkronisasi terdistribusi yang mengimplementasikan empat konsep inti sistem terdistribusi: **Raft Consensus**, **Distributed Locking**, **Distributed Queue dengan Consistent Hashing**, dan **MESI Cache Coherence**.

---

## Identitas Mahasiswa

| Field | Keterangan |
|---|---|
| **Nama** | Muhammad Shadiq Al-Fatiy |
| **NIM** | 11231065 |
| **Kelas** | Sistem Paralel dan Terdistribusi A (SisTer A) |
| **Program Studi** | Informatika |
| **Jurusan** | Teknik Elektro, Informatika, dan Bisnis (JTEIB) |
| **Fakultas** | Sains dan Teknologi Informasi (FSTI) |

---

## Arsitektur Sistem

```
┌────────────────────────────────────────────────────────┐
│                    Docker Network                      │
│                                                        │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐         │
│  │  Node 1  │◄──►│  Node 2  │◄──►│  Node 3  │         │
│  │ :8001    │    │ :8002    │    │ :8003    │         │
│  │          │    │          │    │          │         │
│  │ [Raft  ] │    │ [Raft  ] │    │ [Raft  ] │         │
│  │ [Lock  ] │    │ [Lock  ] │    │ [Lock  ] │         │
│  │ [Queue ] │    │ [Queue ] │    │ [Queue ] │         │
│  │ [Cache ] │    │ [Cache ] │    │ [Cache ] │         │
│  └──────────┘    └──────────┘    └──────────┘         │
│        │               │               │               │
│        └───────────────┼───────────────┘               │
│                        │                               │
│                 ┌──────┴──────┐                        │
│                 │    Redis    │  Penyimpanan Persisten │
│                 │   :6379     │                        │
│                 └─────────────┘                        │
└────────────────────────────────────────────────────────┘
```

Setiap node menjalankan image Docker yang sama dengan konfigurasi berbeda, membentuk kluster 3-node yang dikelola oleh Docker Compose. Semua subsistem (Raft, Lock, Queue, Cache) berjalan di setiap node. Redis digunakan sebagai backing store persisten.

---

## Fitur Utama

### 1. Raft Consensus (Pemilihan Pemimpin)
- Pemilihan leader dengan election timeout acak (1500–3000 ms)
- Replikasi log operasi kunci melalui AppendEntries RPC
- Heartbeat setiap 500 ms untuk mencegah re-election
- Failover otomatis jika leader gagal

### 2. Distributed Lock Manager
- Kunci **Shared (read)** — beberapa client boleh memegang sekaligus
- Kunci **Exclusive (write)** — hanya satu client pada satu waktu
- **Deteksi deadlock** via DFS pada wait-for graph
- **TTL otomatis** untuk mencegah stale lock jika client crash
- Semua operasi kunci direplikasi melalui Raft

### 3. Distributed Queue (Consistent Hashing)
- Hash ring dengan 150 virtual node per node fisik
- Routing pesan berdasarkan hash topik → node pemilik
- Jaminan pengiriman **at-least-once** dengan ack tracking
- Re-delivery otomatis untuk pesan yang tidak di-ack setelah timeout
- Persistensi pesan di Redis

### 4. MESI Cache Coherence
- 4 state penuh: **Modified, Exclusive, Shared, Invalid**
- Protokol snoop via HTTP antar-node (BusRd, BusRdX)
- Write-back policy: entry Modified di-flush ke Redis saat eviksi
- LRU replacement dengan kapasitas yang dapat dikonfigurasi

### 5. Keamanan (Bonus)
- Autentikasi API key dengan RBAC (Admin, Writer, Reader)
- Autentikasi antar-node via shared secret
- Audit logging setiap request

---

## Cara Menjalankan

### Cara 1: Docker Compose (Direkomendasikan)

```bash
# Clone repository
git clone https://github.com/mshadiqaf/distributed-sync-system.git
cd distributed-sync-system

# Jalankan kluster 3-node + Redis
docker-compose -f docker/docker-compose.yml up --build -d

# Verifikasi semua node berjalan
curl http://localhost:8001/health
curl http://localhost:8002/health
curl http://localhost:8003/health

# Hentikan kluster
docker-compose -f docker/docker-compose.yml down
```

### Cara 2: Lokal (Tanpa Docker)

```bash
# Install dependensi
pip install -r requirements.txt

# Jalankan Redis
docker run -d --name redis -p 6379:6379 redis:7-alpine

# Terminal 1 — Node 1
NODE_ID=node1 NODE_PORT=8001 PEER_NODES=http://localhost:8002,http://localhost:8003 \
API_KEY=dev-api-key-123 ADMIN_KEY=dev-admin-key-456 NODE_SECRET=dev-node-secret-789 \
python -m uvicorn src.main:app --host 0.0.0.0 --port 8001

# Terminal 2 — Node 2
NODE_ID=node2 NODE_PORT=8002 PEER_NODES=http://localhost:8001,http://localhost:8003 \
API_KEY=dev-api-key-123 ADMIN_KEY=dev-admin-key-456 NODE_SECRET=dev-node-secret-789 \
python -m uvicorn src.main:app --host 0.0.0.0 --port 8002

# Terminal 3 — Node 3
NODE_ID=node3 NODE_PORT=8003 PEER_NODES=http://localhost:8001,http://localhost:8002 \
API_KEY=dev-api-key-123 ADMIN_KEY=dev-admin-key-456 NODE_SECRET=dev-node-secret-789 \
python -m uvicorn src.main:app --host 0.0.0.0 --port 8003
```

---

## Contoh Penggunaan API

> Swagger UI: `http://localhost:8001/docs` (tanpa autentikasi)

### Health Check

```bash
curl http://localhost:8001/health
```
```json
{"node_id": "node1", "status": "healthy", "role": "leader", "leader_id": "node1"}
```

### Raft State

```bash
curl -H "X-API-Key: dev-api-key-123" http://localhost:8001/raft/state
```
```json
{"node_id": "node1", "role": "leader", "current_term": 2, "leader_id": "node1"}
```

### Distributed Lock

```bash
# Akuisisi kunci exclusive
curl -X POST http://localhost:8001/lock/acquire \
  -H "X-API-Key: dev-api-key-123" -H "Content-Type: application/json" \
  -d '{"resource": "file.db", "client_id": "app1", "lock_type": "exclusive", "ttl": 30}'
```
```json
{"status": "granted", "resource": "file.db", "client_id": "app1", "lock_type": "exclusive"}
```

```bash
# Akuisisi kunci shared (dua client bersamaan)
curl -X POST http://localhost:8001/lock/acquire \
  -H "X-API-Key: dev-api-key-123" -H "Content-Type: application/json" \
  -d '{"resource": "config.json", "client_id": "reader1", "lock_type": "shared"}'

# Lihat semua kunci aktif
curl -H "X-API-Key: dev-api-key-123" http://localhost:8001/lock/status

# Lepaskan kunci
curl -X POST http://localhost:8001/lock/release \
  -H "X-API-Key: dev-api-key-123" -H "Content-Type: application/json" \
  -d '{"resource": "file.db", "client_id": "app1"}'
```

### Distributed Queue

```bash
# Kirim pesan ke topik "orders"
curl -X POST http://localhost:8001/queue/push \
  -H "X-API-Key: dev-api-key-123" -H "Content-Type: application/json" \
  -d '{"topic": "orders", "data": {"order_id": 101, "item": "laptop"}, "producer_id": "shop"}'
```
```json
{"status": "queued", "message_id": "a1b2c3d4", "topic": "orders", "node": "node2"}
```

```bash
# Consume pesan
curl -X POST http://localhost:8001/queue/consume \
  -H "X-API-Key: dev-api-key-123" -H "Content-Type: application/json" \
  -d '{"topic": "orders", "consumer_id": "worker1"}'

# Ack pesan
curl -X POST http://localhost:8001/queue/ack \
  -H "X-API-Key: dev-api-key-123" -H "Content-Type: application/json" \
  -d '{"message_id": "a1b2c3d4", "consumer_id": "worker1"}'
```

### MESI Cache

```bash
# Tulis ke Node 1 → state M (Modified)
curl -X PUT http://localhost:8001/cache/user:42 \
  -H "X-API-Key: dev-api-key-123" -H "Content-Type: application/json" \
  -d '{"value": "John Doe"}'
```
```json
{"status": "written", "key": "user:42", "state": "M", "node": "node1"}
```

```bash
# Baca dari Node 2 → BusRd, state jadi S (Shared)
curl -H "X-API-Key: dev-api-key-123" http://localhost:8002/cache/user:42
```
```json
{"status": "miss_peer", "key": "user:42", "value": "John Doe", "state": "S", "node": "node2"}
```

```bash
# Lihat statistik cache
curl -H "X-API-Key: dev-api-key-123" http://localhost:8001/cache-stats
```

---

## Daftar Endpoint API

### System
| Method | Endpoint | Deskripsi |
|---|---|---|
| GET | `/health` | Status kesehatan node |
| GET | `/peers` | Daftar peer yang dikenal |
| GET | `/metrics` | Metrik performa |
| GET | `/cluster/status` | Status seluruh kluster |

### Raft Consensus
| Method | Endpoint | Deskripsi |
|---|---|---|
| GET | `/raft/state` | Status Raft (role, term, leader) |
| POST | `/raft/request-vote` | RPC pemilihan (internal) |
| POST | `/raft/append-entries` | RPC heartbeat + replikasi (internal) |

### Lock Manager
| Method | Endpoint | Deskripsi |
|---|---|---|
| POST | `/lock/acquire` | Akuisisi kunci terdistribusi |
| POST | `/lock/release` | Lepaskan kunci |
| GET | `/lock/status` | Semua kunci aktif |
| GET | `/lock/deadlocks` | Analisis wait-for graph |

### Queue System
| Method | Endpoint | Deskripsi |
|---|---|---|
| POST | `/queue/push` | Kirim pesan ke topik |
| POST | `/queue/consume` | Ambil pesan dari topik |
| POST | `/queue/ack` | Konfirmasi pesan diproses |
| GET | `/queue/status` | Kedalaman antrean dan topik |
| GET | `/queue/ring` | Visualisasi hash ring |

### Cache (MESI)
| Method | Endpoint | Deskripsi |
|---|---|---|
| GET | `/cache/{key}` | Baca dengan protokol MESI |
| PUT | `/cache/{key}` | Tulis dengan protokol MESI |
| DELETE | `/cache/{key}` | Invalidasi entry cache |
| GET | `/cache-stats` | Distribusi state MESI |
| GET | `/cache-entries` | Semua entry dengan state |

### Security
| Method | Endpoint | Deskripsi |
|---|---|---|
| GET | `/audit/logs` | Riwayat akses (admin only) |

---

## Hasil Pengujian

### Unit Test (Tanpa Kluster)

```
$ python -m pytest tests/unit/ -v

tests/unit/test_cache.py          21 passed
tests/unit/test_lock_manager.py   17 passed
tests/unit/test_metrics.py         5 passed
tests/unit/test_queue.py          20 passed

======================== 63 passed in 1.60s ========================
```

### Menjalankan Test

```bash
# Unit test saja (tidak butuh kluster)
python -m pytest tests/unit/ -v

# Semua test (butuh kluster berjalan untuk integration test)
python -m pytest tests/ -v

# Test komponen tertentu
python -m pytest tests/unit/test_lock_manager.py -v
python -m pytest tests/unit/test_cache.py -v
```

### Benchmark

```bash
# Jalankan kluster dulu, kemudian:
python benchmarks/benchmark_runner.py   # hasil di benchmarks/results/
python benchmarks/visualize.py          # grafik di benchmarks/graphs/
```

---

## Struktur Proyek

```
distributed-sync-system/
├── src/
│   ├── main.py                     # Entry point (uvicorn runner)
│   ├── nodes/
│   │   ├── base_node.py            # FastAPI factory + semua endpoint
│   │   ├── lock_manager.py         # Distributed Lock Manager
│   │   ├── queue_node.py           # Queue + Consistent Hash Ring
│   │   └── cache_node.py           # MESI Cache Coherence
│   ├── consensus/
│   │   └── raft.py                 # Implementasi Raft (511 baris)
│   ├── communication/
│   │   ├── message_passing.py      # HTTP client antar-node (retry, backoff)
│   │   └── failure_detector.py     # Deteksi kegagalan node (heartbeat)
│   └── utils/
│       ├── config.py               # Pydantic settings (env vars)
│       ├── metrics.py              # Collector metrik in-memory
│       └── security.py             # RBAC + API key + audit logging
├── docs/
│   ├── architecture.md             # Penjelasan arsitektur lengkap
│   ├── api_spec.md                 # Spesifikasi API dengan contoh JSON
│   └── deployment_guide.md         # Panduan deploy + troubleshooting
├── docker/
│   ├── Dockerfile.node             # Image Docker satu node
│   └── docker-compose.yml          # Orkestrasi 3-node + Redis
├── benchmarks/
│   ├── benchmark_runner.py         # Suite benchmark performa
│   └── visualize.py                # Generator grafik matplotlib
├── tests/
│   ├── unit/                       # Unit test (tidak butuh kluster)
│   │   ├── test_lock_manager.py
│   │   ├── test_queue.py
│   │   ├── test_cache.py
│   │   └── test_metrics.py
│   └── integration/
│       └── test_cluster.py         # Integration test (butuh kluster)
├── pytest.ini                      # Konfigurasi pytest
├── requirements.txt                # Dependensi Python
├── .env.example                    # Template environment variables
└── README.md
```

---

## Konfigurasi Environment Variables

Lihat `.env.example` untuk daftar lengkap. Variabel utama:

| Variabel | Default | Deskripsi |
|---|---|---|
| `NODE_ID` | — | ID unik node |
| `NODE_PORT` | `8001` | Port HTTP |
| `PEER_NODES` | — | URL peer (koma-dipisah) |
| `REDIS_URL` | `redis://localhost:6379/0` | Koneksi Redis |
| `API_KEY` | `dev-api-key-123` | API key (role writer) |
| `ADMIN_KEY` | `dev-admin-key-456` | API key (role admin) |
| `NODE_SECRET` | `dev-node-secret-789` | Secret antar-node |
| `ELECTION_TIMEOUT_MIN` | `1500` | Min timeout Raft (ms) |
| `ELECTION_TIMEOUT_MAX` | `3000` | Max timeout Raft (ms) |
| `HEARTBEAT_INTERVAL` | `500` | Interval heartbeat (ms) |
| `CACHE_MAX_SIZE` | `1000` | Kapasitas cache per node |

---

## Troubleshooting

**Node tidak merespons:**
```bash
docker-compose -f docker/docker-compose.yml logs node1
```

**Tidak ada leader Raft:** Tunggu 3–5 detik setelah startup. Cek dengan `curl http://localhost:8001/raft/state`.

**Lock selalu return "not_leader":** Kirim request ke node yang menjadi leader. Temukan leader dari `/raft/state`.

**Redis connection error:** Pastikan container Redis berjalan: `docker ps | grep redis`

Panduan lengkap: [docs/deployment_guide.md](docs/deployment_guide.md)

---

## Tech Stack

| Komponen | Teknologi |
|---|---|
| Bahasa | Python 3.12+ |
| Framework | FastAPI (async) |
| State Store | Redis 7 |
| HTTP Client | httpx (async) |
| Container | Docker & Docker Compose |
| Testing | pytest + pytest-asyncio |
| Visualisasi | matplotlib |

---

## Dokumentasi Teknis

- [docs/architecture.md](docs/architecture.md) — Arsitektur sistem, diagram, penjelasan protokol
- [docs/api_spec.md](docs/api_spec.md) — Spesifikasi API lengkap dengan contoh request/response
- [docs/api_spec.yaml](docs/api_spec.yaml) — OpenAPI 3.0 Specification (Swagger/Postman compatible)
- [docs/deployment_guide.md](docs/deployment_guide.md) — Panduan deployment dan troubleshooting
