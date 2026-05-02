# Panduan Deployment — Distributed Synchronization System

## Prasyarat

| Software | Versi Minimum | Digunakan untuk |
|---|---|---|
| Python | 3.12+ | Menjalankan node secara lokal |
| Docker | 24+ | Deployment via container |
| Docker Compose | 2.20+ | Orkestrasi multi-node |

---

## Cara 1: Docker Compose (Direkomendasikan)

Cara termudah dan paling mendekati lingkungan produksi. Menjalankan 3 node + Redis sekaligus.

### Langkah-langkah

```bash
# 1. Clone repository dan masuk ke direktori
git clone <url-repo>
cd distributed-sync-system

# 2. Salin file konfigurasi (opsional — docker-compose sudah punya default)
cp .env.example .env

# 3. Build dan jalankan semua container
docker-compose -f docker/docker-compose.yml up --build -d

# 4. Verifikasi semua node berjalan
curl http://localhost:8001/health
curl http://localhost:8002/health
curl http://localhost:8003/health
```

### Verifikasi Kluster Sehat

```bash
# Cek status kluster dari node1
curl http://localhost:8001/cluster/status

# Cek siapa yang menjadi leader Raft
curl http://localhost:8001/raft/state
curl http://localhost:8002/raft/state
curl http://localhost:8003/raft/state
```

### Menghentikan Kluster

```bash
# Hentikan semua container
docker-compose -f docker/docker-compose.yml down

# Hentikan dan hapus volume (reset data Redis)
docker-compose -f docker/docker-compose.yml down -v
```

### Melihat Log Container

```bash
# Log semua container
docker-compose -f docker/docker-compose.yml logs -f

# Log node tertentu saja
docker-compose -f docker/docker-compose.yml logs -f node1
docker-compose -f docker/docker-compose.yml logs -f redis
```

---

## Cara 2: Menjalankan Secara Lokal

Cocok untuk development dan debugging.

### Langkah-langkah

**1. Install dependensi Python:**
```bash
pip install -r requirements.txt
```

**2. Jalankan Redis (via Docker):**
```bash
docker run -d --name redis -p 6379:6379 redis:7-alpine
```

**3. Jalankan 3 node di terminal terpisah:**

Terminal 1 — Node 1:
```bash
NODE_ID=node1 \
NODE_PORT=8001 \
PEER_NODES=http://localhost:8002,http://localhost:8003 \
REDIS_URL=redis://localhost:6379/0 \
API_KEY=dev-api-key-123 \
ADMIN_KEY=dev-admin-key-456 \
NODE_SECRET=dev-node-secret-789 \
python -m uvicorn src.main:app --host 0.0.0.0 --port 8001
```

Terminal 2 — Node 2:
```bash
NODE_ID=node2 \
NODE_PORT=8002 \
PEER_NODES=http://localhost:8001,http://localhost:8003 \
REDIS_URL=redis://localhost:6379/0 \
API_KEY=dev-api-key-123 \
ADMIN_KEY=dev-admin-key-456 \
NODE_SECRET=dev-node-secret-789 \
python -m uvicorn src.main:app --host 0.0.0.0 --port 8002
```

Terminal 3 — Node 3:
```bash
NODE_ID=node3 \
NODE_PORT=8003 \
PEER_NODES=http://localhost:8001,http://localhost:8002 \
REDIS_URL=redis://localhost:6379/0 \
API_KEY=dev-api-key-123 \
ADMIN_KEY=dev-admin-key-456 \
NODE_SECRET=dev-node-secret-789 \
python -m uvicorn src.main:app --host 0.0.0.0 --port 8003
```

---

## Environment Variables

Semua konfigurasi dapat diatur via environment variables atau file `.env`:

| Variabel | Default (lokal) | Deskripsi |
|---|---|---|
| `NODE_ID` | — | ID unik node (`node1`, `node2`, dst) |
| `NODE_HOST` | `0.0.0.0` | Host binding |
| `NODE_PORT` | `8001` | Port HTTP |
| `PEER_NODES` | — | URL peer dipisahkan koma |
| `REDIS_URL` | `redis://localhost:6379/0` | Koneksi Redis |
| `API_KEY` | `dev-api-key-123` | API key untuk role writer |
| `ADMIN_KEY` | `dev-admin-key-456` | API key untuk role admin |
| `NODE_SECRET` | `dev-node-secret-789` | Secret untuk komunikasi antar-node |
| `ELECTION_TIMEOUT_MIN` | `1500` | Minimum election timeout Raft (ms) |
| `ELECTION_TIMEOUT_MAX` | `3000` | Maximum election timeout Raft (ms) |
| `HEARTBEAT_INTERVAL` | `500` | Interval heartbeat Raft (ms) |
| `CACHE_MAX_SIZE` | `1000` | Kapasitas maksimum cache per node |
| `CACHE_TTL` | `300` | TTL cache (detik) |
| `QUEUE_ACK_TIMEOUT` | `30` | Batas waktu ack pesan (detik) |
| `QUEUE_VIRTUAL_NODES` | `150` | Jumlah virtual node di hash ring |

**Catatan keamanan:** Ganti semua nilai default (`dev-*`) dengan nilai acak yang kuat di lingkungan produksi.

---

## Menjalankan Test

```bash
# Install dependensi test
pip install pytest pytest-asyncio

# Jalankan semua test
python -m pytest tests/ -v

# Jalankan satu kelas test saja
python -m pytest tests/unit/test_lock_manager.py -v
python -m pytest tests/unit/test_cache.py -v
python -m pytest tests/unit/test_queue.py -v

# Jalankan dengan output lengkap
python -m pytest tests/ -v --tb=short
```

---

## Menjalankan Benchmark

Kluster harus sudah berjalan sebelum menjalankan benchmark.

```bash
# Jalankan benchmark (lock, queue, cache)
python benchmarks/benchmark_runner.py

# Buat visualisasi grafik dari hasil benchmark
python benchmarks/visualize.py
# Output: benchmarks/graphs/
```

---

## Akses API

Setelah kluster berjalan, API tersedia di:

| URL | Keterangan |
|---|---|
| `http://localhost:8001` | Node 1 |
| `http://localhost:8002` | Node 2 |
| `http://localhost:8003` | Node 3 |
| `http://localhost:8001/docs` | Swagger UI Node 1 (tanpa auth) |
| `http://localhost:8001/redoc` | ReDoc Node 1 |

---

## Contoh Penggunaan curl

### System

```bash
# Cek health node
curl http://localhost:8001/health

# Cek status kluster
curl -H "X-API-Key: dev-api-key-123" http://localhost:8001/cluster/status
```

### Lock Manager

```bash
# Akuisisi kunci exclusive
curl -X POST http://localhost:8001/lock/acquire \
  -H "X-API-Key: dev-api-key-123" \
  -H "Content-Type: application/json" \
  -d '{"resource": "file.txt", "client_id": "app1", "lock_type": "exclusive", "ttl": 30}'

# Akuisisi kunci shared
curl -X POST http://localhost:8001/lock/acquire \
  -H "X-API-Key: dev-api-key-123" \
  -H "Content-Type: application/json" \
  -d '{"resource": "file.txt", "client_id": "app2", "lock_type": "shared"}'

# Lihat semua kunci aktif
curl -H "X-API-Key: dev-api-key-123" http://localhost:8001/lock/status

# Lepaskan kunci
curl -X POST http://localhost:8001/lock/release \
  -H "X-API-Key: dev-api-key-123" \
  -H "Content-Type: application/json" \
  -d '{"resource": "file.txt", "client_id": "app1"}'
```

### Queue System

```bash
# Kirim pesan
curl -X POST http://localhost:8001/queue/push \
  -H "X-API-Key: dev-api-key-123" \
  -H "Content-Type: application/json" \
  -d '{"topic": "orders", "data": {"id": 1, "item": "laptop"}, "producer_id": "shop"}'

# Consume pesan
curl -X POST http://localhost:8001/queue/consume \
  -H "X-API-Key: dev-api-key-123" \
  -H "Content-Type: application/json" \
  -d '{"topic": "orders", "consumer_id": "worker1"}'

# Ack pesan (ganti <message_id> dengan ID dari response consume)
curl -X POST http://localhost:8001/queue/ack \
  -H "X-API-Key: dev-api-key-123" \
  -H "Content-Type: application/json" \
  -d '{"message_id": "<message_id>", "consumer_id": "worker1"}'
```

### Cache (MESI)

```bash
# Tulis ke cache
curl -X PUT http://localhost:8001/cache/user:42 \
  -H "X-API-Key: dev-api-key-123" \
  -H "Content-Type: application/json" \
  -d '{"value": "John Doe"}'

# Baca dari cache (node 1)
curl -H "X-API-Key: dev-api-key-123" http://localhost:8001/cache/user:42

# Baca dari cache (node 2) — akan terjadi snoop BusRd, state jadi Shared
curl -H "X-API-Key: dev-api-key-123" http://localhost:8002/cache/user:42

# Lihat statistik cache
curl -H "X-API-Key: dev-api-key-123" http://localhost:8001/cache-stats

# Invalidasi entry
curl -X DELETE http://localhost:8001/cache/user:42 \
  -H "X-API-Key: dev-admin-key-456"
```

---

## Troubleshooting

### Node tidak bisa connect ke Redis

**Gejala:** Log menampilkan `Redis not available`

**Solusi:**
```bash
# Pastikan Redis berjalan
docker ps | grep redis

# Test koneksi Redis
docker exec -it <redis-container-id> redis-cli ping
# Harus return: PONG
```

### Tidak ada leader Raft terpilih

**Gejala:** Semua node menampilkan `role: follower` di `/raft/state`

**Solusi:**
- Tunggu 3–5 detik (election timeout butuh waktu)
- Pastikan semua node bisa saling berkomunikasi
- Cek log untuk pesan `RequestVote` atau `ELECTED AS LEADER`

```bash
# Cek konektivitas antar node (di Docker)
docker exec node1 curl http://node2:8002/health
docker exec node1 curl http://node3:8003/health
```

### Lock selalu return "not_leader"

**Gejala:** `/lock/acquire` selalu mengembalikan `"reason": "not_leader"`

**Solusi:** Kirim request lock ke node yang sedang menjadi Leader:
```bash
# Temukan leader dulu
curl http://localhost:8001/raft/state | grep leader_id

# Kirim ke leader (misalnya node1 = port 8001)
curl -X POST http://localhost:8001/lock/acquire ...
```

### Port sudah digunakan

**Gejala:** `Error: address already in use`

**Solusi:**
```bash
# Temukan proses yang menggunakan port
netstat -ano | findstr :8001    # Windows
lsof -i :8001                   # Linux/Mac

# Hentikan container lama
docker-compose -f docker/docker-compose.yml down
```

### Container terus restart

**Gejala:** `docker ps` menampilkan status `Restarting`

**Solusi:**
```bash
# Lihat log container
docker logs node1 --tail 50

# Rebuild dari awal
docker-compose -f docker/docker-compose.yml down -v
docker-compose -f docker/docker-compose.yml up --build
```
