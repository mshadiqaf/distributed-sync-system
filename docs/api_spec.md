# Spesifikasi API — Distributed Synchronization System

Semua endpoint membutuhkan autentikasi kecuali yang ditandai **[Public]**.

**Header autentikasi:** `X-API-Key: <api_key>`  
**Base URL:** `http://localhost:8001` (sesuaikan nomor port untuk node lain)

---

## System Endpoints

### GET /health [Public]
Cek status kesehatan node.

**Response:**
```json
{
  "node_id": "node1",
  "status": "healthy",
  "uptime_seconds": 42.5,
  "port": 8001,
  "role": "leader",
  "leader_id": "node1"
}
```

---

### GET /peers
Daftar semua peer yang dikenal.

**Response:**
```json
{
  "node_id": "node1",
  "peers": ["http://node2:8002", "http://node3:8003"],
  "peer_count": 2
}
```

---

### GET /metrics
Semua metrik node (request count, latency, lock, queue, cache, raft).

**Response:**
```json
{
  "node_id": "node1",
  "request_count": 150,
  "latency": { "avg_ms": 12.3, "p95_ms": 45.0 },
  "lock": { "acquired": 10, "released": 8, "active_locks": 2 },
  "queue": { "messages_pushed": 50, "messages_consumed": 48 },
  "cache": { "hits": 80, "misses": 20, "hit_rate_pct": 80.0 },
  "raft": { "term": 3, "role": "leader" }
}
```

---

### GET /cluster/status
Status kesehatan seluruh kluster.

**Response:**
```json
{
  "node_id": "node1",
  "cluster": {
    "http://node2:8002": "ALIVE",
    "http://node3:8003": "ALIVE"
  },
  "alive_peers": ["http://node2:8002", "http://node3:8003"],
  "total_peers": 2
}
```

---

## Raft Consensus Endpoints

### GET /raft/state
Status konsensus Raft saat ini.

**Response:**
```json
{
  "node_id": "node1",
  "role": "leader",
  "current_term": 3,
  "leader_id": "node1",
  "log_length": 15,
  "commit_index": 15
}
```

---

### POST /raft/request-vote [Internal — Node Secret]
RPC pemilihan Raft antar-node.

**Request:**
```json
{
  "term": 4,
  "candidate_id": "node2",
  "last_log_index": 15,
  "last_log_term": 3
}
```

**Response:**
```json
{
  "term": 4,
  "vote_granted": true
}
```

---

### POST /raft/append-entries [Internal — Node Secret]
RPC heartbeat dan replikasi log Raft.

**Request:**
```json
{
  "term": 3,
  "leader_id": "node1",
  "prev_log_index": 14,
  "prev_log_term": 3,
  "entries": [],
  "leader_commit": 14
}
```

**Response:**
```json
{
  "term": 3,
  "success": true
}
```

---

## Lock Manager Endpoints

### POST /lock/acquire
Akuisisi kunci terdistribusi. Hanya diproses oleh Leader Raft.

**Request:**
```json
{
  "resource": "database_table_users",
  "client_id": "service-A",
  "lock_type": "exclusive",
  "ttl": 30.0,
  "timeout": 10.0
}
```

| Field | Tipe | Default | Deskripsi |
|---|---|---|---|
| `resource` | string | (wajib) | Nama resource yang dikunci |
| `client_id` | string | (wajib) | Identitas client |
| `lock_type` | string | `"exclusive"` | `"shared"` atau `"exclusive"` |
| `ttl` | float | 30.0 | Masa hidup kunci (detik) |
| `timeout` | float | 10.0 | Batas tunggu akuisisi (detik) |

**Response — Berhasil:**
```json
{
  "status": "granted",
  "resource": "database_table_users",
  "client_id": "service-A",
  "lock_type": "exclusive",
  "granted_at": 1704067200.0,
  "ttl": 30.0
}
```

**Response — Timeout:**
```json
{
  "status": "timeout",
  "resource": "database_table_users",
  "client_id": "service-A",
  "message": "Lock acquisition timed out after 10.0s"
}
```

**Response — Deadlock:**
```json
{
  "status": "deadlock",
  "reason": "deadlock_detected",
  "cycle": ["service-A", "service-B", "service-A"],
  "message": "Deadlock detected, request aborted"
}
```

**Response — Bukan Leader:**
```json
{
  "status": "denied",
  "reason": "not_leader",
  "leader_id": "node1",
  "message": "Forward request to leader: node1"
}
```

---

### POST /lock/release
Lepaskan kunci yang dipegang.

**Request:**
```json
{
  "resource": "database_table_users",
  "client_id": "service-A"
}
```

**Response:**
```json
{
  "status": "released",
  "resource": "database_table_users",
  "client_id": "service-A"
}
```

---

### GET /lock/status
Semua kunci aktif dan request yang menunggu.

**Response:**
```json
{
  "active_locks": [
    {
      "resource": "database_table_users",
      "client_id": "service-A",
      "lock_type": "exclusive",
      "granted": true,
      "granted_at": 1704067200.0,
      "ttl": 30.0,
      "expired": false
    }
  ],
  "active_count": 1,
  "waiting_requests": [],
  "waiting_count": 0,
  "resources_locked": ["database_table_users"]
}
```

---

### GET /lock/deadlocks
Wait-for graph untuk analisis deadlock.

**Response:**
```json
{
  "wait_for_graph": {
    "service-A": ["service-B"],
    "service-B": ["service-A"]
  },
  "waiting_clients": ["service-A", "service-B"]
}
```

---

## Queue System Endpoints

### POST /queue/push
Kirim pesan ke topik antrean.

**Request:**
```json
{
  "topic": "order-processing",
  "data": { "order_id": 123, "amount": 50000 },
  "producer_id": "payment-service",
  "priority": 0
}
```

| Field | Tipe | Default | Deskripsi |
|---|---|---|---|
| `topic` | string | `"default"` | Nama topik antrean |
| `data` | any | null | Payload pesan |
| `producer_id` | string | `"anonymous"` | Identitas producer |
| `priority` | int | 0 | Prioritas pesan |

**Response:**
```json
{
  "status": "queued",
  "message_id": "a1b2c3d4",
  "topic": "order-processing",
  "node": "node2",
  "queue_depth": 5
}
```

---

### POST /queue/consume
Ambil satu pesan dari topik.

**Request:**
```json
{
  "topic": "order-processing",
  "consumer_id": "worker-1"
}
```

**Response — Ada pesan:**
```json
{
  "status": "delivered",
  "message_id": "a1b2c3d4",
  "topic": "order-processing",
  "data": { "order_id": 123, "amount": 50000 },
  "delivery_count": 1,
  "consumer_id": "worker-1"
}
```

**Response — Kosong:**
```json
{
  "status": "empty",
  "topic": "order-processing",
  "message": "No messages available"
}
```

---

### POST /queue/ack
Konfirmasi bahwa pesan sudah diproses.

**Request:**
```json
{
  "message_id": "a1b2c3d4",
  "consumer_id": "worker-1"
}
```

**Response:**
```json
{
  "status": "acked",
  "message_id": "a1b2c3d4",
  "topic": "order-processing"
}
```

---

### GET /queue/status
Status semua topik dan antrean.

**Response:**
```json
{
  "node_id": "node1",
  "topics": {
    "order-processing": {
      "depth": 3,
      "owner_node": "http://node2:8002",
      "consumers": ["worker-1"]
    }
  },
  "total_messages": 3,
  "pending_acks": 1,
  "hash_ring": {
    "total_vnodes": 450,
    "nodes": ["http://node1:8001", "http://node2:8002", "http://node3:8003"],
    "node_count": 3
  }
}
```

---

### GET /queue/ring
Informasi distribusi hash ring.

**Response:**
```json
{
  "total_vnodes": 450,
  "nodes": ["http://node1:8001", "http://node2:8002", "http://node3:8003"],
  "node_count": 3,
  "distribution": {
    "http://node1:8001": 150,
    "http://node2:8002": 150,
    "http://node3:8003": 150
  }
}
```

---

## Cache (MESI) Endpoints

### GET /cache/{key}
Baca nilai dari cache terdistribusi (protokol MESI).

**Contoh:** `GET /cache/user:42`

**Response — Cache Hit:**
```json
{
  "status": "hit",
  "key": "user:42",
  "value": "John Doe",
  "state": "E",
  "node": "node1"
}
```

**Response — Miss dari Peer:**
```json
{
  "status": "miss_peer",
  "key": "user:42",
  "value": "John Doe",
  "state": "S",
  "node": "node1"
}
```

**Response — Miss dari Store:**
```json
{
  "status": "miss_store",
  "key": "user:42",
  "value": "John Doe",
  "state": "E",
  "node": "node1"
}
```

**Response — Tidak Ditemukan:**
```json
{
  "status": "not_found",
  "key": "user:42",
  "value": null,
  "node": "node1"
}
```

---

### PUT /cache/{key}
Tulis nilai ke cache (protokol MESI: invalidasi peer terlebih dahulu).

**Contoh:** `PUT /cache/user:42`

**Request:**
```json
{
  "value": "Jane Doe"
}
```

**Response:**
```json
{
  "status": "written",
  "key": "user:42",
  "value": "Jane Doe",
  "state": "M",
  "node": "node1"
}
```

---

### DELETE /cache/{key}
Invalidasi entry cache di semua node.

**Response:**
```json
{
  "status": "invalidated",
  "key": "user:42",
  "node": "node1",
  "was_cached": true,
  "old_state": "S"
}
```

---

### GET /cache-stats
Statistik cache dan distribusi state MESI.

**Response:**
```json
{
  "node_id": "node1",
  "total_entries": 42,
  "max_size": 1000,
  "utilization_pct": 4.2,
  "state_distribution": { "M": 5, "E": 20, "S": 15, "I": 2 },
  "hit_rate_pct": 85.3,
  "total_accesses": 100,
  "evictions": 0,
  "invalidations": 3
}
```

---

### GET /cache-entries
Semua entry cache dengan state MESI masing-masing.

**Response:**
```json
{
  "node_id": "node1",
  "entries": [
    {
      "key": "user:42",
      "value": "Jane Doe",
      "state": "M",
      "last_accessed": 1704067200.0,
      "last_modified": 1704067200.0
    }
  ],
  "count": 1
}
```

---

## Security & Audit Endpoints

### GET /audit/logs [Admin Only]
Riwayat akses terbaru (100 entri terakhir).

**Response:**
```json
{
  "entries": [
    {
      "timestamp": 1704067200.0,
      "client_ip": "172.18.0.1",
      "method": "POST",
      "path": "/lock/acquire",
      "role": "writer",
      "status_code": 200,
      "latency_ms": 12.5,
      "user_agent": "curl/8.0"
    }
  ],
  "total": 1
}
```

---

## Kode Status HTTP

| Kode | Kondisi |
|---|---|
| 200 | Sukses |
| 401 | API Key tidak valid atau tidak ada |
| 403 | Role tidak punya izin untuk method ini |
| 404 | Resource/key tidak ditemukan |
| 500 | Error internal server |
