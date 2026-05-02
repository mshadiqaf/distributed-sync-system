# Arsitektur Sistem — Distributed Synchronization System

## Gambaran Umum

Sistem ini mengimplementasikan empat primitif sistem terdistribusi di atas sebuah kluster 3-node:

```
┌─────────────────────────────────────────────────────────┐
│                  Docker Compose Network                 │
│                                                         │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐          │
│  │  Node 1  │◄──►│  Node 2  │◄──►│  Node 3  │          │
│  │ :8001    │    │ :8002    │    │ :8003    │          │
│  │          │    │          │    │          │          │
│  │ [Raft  ] │    │ [Raft  ] │    │ [Raft  ] │ Konsensus│
│  │ [Lock  ] │    │ [Lock  ] │    │ [Lock  ] │ Pengunci │
│  │ [Queue ] │    │ [Queue ] │    │ [Queue ] │ Antrean  │
│  │ [Cache ] │    │ [Cache ] │    │ [Cache ] │ Cache    │
│  └──────────┘    └──────────┘    └──────────┘          │
│        │               │               │                │
│        └───────────────┼───────────────┘                │
│                        │                                │
│                 ┌──────▼──────┐                         │
│                 │    Redis    │  Penyimpanan Persisten  │
│                 │   :6379     │                         │
│                 └─────────────┘                         │
└─────────────────────────────────────────────────────────┘
```

Setiap node menjalankan aplikasi FastAPI yang sama (entry point: `src/main.py`). Semua subsistem diinisialisasi di `src/nodes/base_node.py:create_app()` dan disimpan sebagai variabel global modul.

---

## 1. Konsensus Raft (`src/consensus/raft.py`)

### Tujuan
Memilih satu pemimpin (leader) di antara node-node dalam kluster dan mereplikasi operasi kritis (seperti akuisisi kunci) ke semua node agar sistem tetap konsisten meskipun ada kegagalan node.

### Peran Node
```
Follower → Candidate → Leader
```

- **Follower**: Menerima heartbeat dari leader. Jika tidak ada heartbeat dalam election timeout, beralih ke Candidate.
- **Candidate**: Meminta suara dari semua node. Jika mendapat mayoritas, menjadi Leader.
- **Leader**: Mengirim heartbeat setiap `HEARTBEAT_INTERVAL` ms dan mereplikasi log ke semua Follower.

### State yang Dipersistensikan ke Redis
| Field | Deskripsi |
|---|---|
| `current_term` | Term Raft saat ini (meningkat monoton) |
| `voted_for` | ID node yang mendapat suara di term saat ini |
| `log` | List `LogEntry` (term, index, operation, data) |

### Alur Pemilihan Pemimpin
1. Follower tidak menerima heartbeat → election timeout (acak 1500–3000 ms)
2. Node menjadi Candidate, increment `current_term`, kirim `RequestVote` ke semua peer
3. Peer memberikan suara jika: term lebih tinggi DAN log candidate tidak lebih ketinggalan
4. Candidate mendapat suara mayoritas → menjadi Leader
5. Leader mengirim `AppendEntries` kosong (heartbeat) setiap 500 ms

### Alur Replikasi Log
1. Client request masuk ke Leader (lock acquire/release)
2. Leader menambah entry ke log lokal
3. Leader mengirim `AppendEntries` ke semua Follower
4. Setelah mayoritas Follower mengonfirmasi, entry di-commit
5. Leader menerapkan operasi dan mengembalikan respons ke client

### Endpoint
| Method | Path | Deskripsi |
|---|---|---|
| POST | `/raft/request-vote` | RPC pemilihan (internal, perlu node-secret) |
| POST | `/raft/append-entries` | RPC heartbeat + replikasi (internal) |
| GET | `/raft/state` | Status Raft saat ini |

---

## 2. Distributed Lock Manager (`src/nodes/lock_manager.py`)

### Tujuan
Menyediakan kunci terdistribusi dengan semantik shared (baca) dan exclusive (tulis), dilengkapi deteksi deadlock otomatis.

### Jenis Kunci
| Jenis | Deskripsi | Kompatibilitas |
|---|---|---|
| `shared` | Beberapa reader boleh memegang sekaligus | shared + shared ✓ |
| `exclusive` | Hanya satu pemegang pada satu waktu | exclusive + apapun ✗ |

### Struktur Data Internal
```
granted_locks:  { resource → [LockRequest, ...] }
waiting_locks:  { resource → [LockRequest, ...] }
wait_for_graph: { client_id → set(client_id yang ditunggu) }
```

### Alur Akuisisi Kunci
```
Client → POST /lock/acquire
         ↓
    [Apakah node adalah Leader Raft?]
         Tidak → Return "not_leader", arahkan ke Leader
         Ya  ↓
    [Bisakah kunci langsung diberikan?]
         Ya  → Grant, propose ke Raft, return "granted"
         Tidak → Tambahkan ke wait queue
                ↓
         [Deteksi deadlock (DFS)]
                Deadlock → Hapus dari queue, return "deadlock"
                Tidak → Tunggu sampai timeout (polling setiap 100ms)
                         Timeout → return "timeout"
                         Granted → return "granted"
```

### Deteksi Deadlock
Menggunakan DFS (Depth-First Search) pada *wait-for graph*:
- Edge `A → B` berarti: client A menunggu client B melepas kunci
- Siklus di graf ini = deadlock
- Ketika deadlock terdeteksi, request yang baru masuk dibatalkan

### TTL & Pembersihan Otomatis
- Setiap kunci memiliki TTL (default 30 detik)
- Background task berjalan setiap 5 detik untuk membersihkan kunci kadaluarsa
- Mencegah situasi "stale lock" jika client crash

### Endpoint
| Method | Path | Deskripsi |
|---|---|---|
| POST | `/lock/acquire` | Akuisisi kunci |
| POST | `/lock/release` | Lepaskan kunci |
| GET | `/lock/status` | Semua kunci aktif dan waiting |
| GET | `/lock/deadlocks` | Wait-for graph saat ini |

---

## 3. Distributed Queue (`src/nodes/queue_node.py`)

### Tujuan
Antrean pesan berbasis topik dengan jaminan pengiriman at-least-once, didistribusikan ke node menggunakan consistent hashing.

### Consistent Hash Ring
```
ConsistentHashRing:
  - Hash function: MD5 dari f"{node_url}:vnode:{i}"
  - Virtual nodes per node fisik: 150 (default)
  - Struktur: List terurut dari (hash_value, node_url)
  - Pencarian: bisect_right → node bertanggung jawab atas topik

Routing topik: hash(topic_name) → cari di ring → node pemilik
```

Dengan 150 virtual nodes per node fisik, distribusi topik menjadi merata (~100 topik per node untuk 300 topik).

### Alur Push Pesan
```
Client → POST /queue/push { topic, data, producer_id }
         ↓
    hash_ring.get_node(topic) → target_node
         ↓
    target == this_node? → simpan lokal + persist ke Redis
    target != this_node? → forward HTTP ke target_node
                           Gagal? → simpan lokal (fallback)
```

### Alur Consume & Ack
```
Client → POST /queue/consume { topic, consumer_id }
         ↓
    Pop message dari depan queue → pindah ke pending_acks
    return { message_id, data, delivery_count }
         ↓
Client → POST /queue/ack { message_id, consumer_id }
         ↓
    Hapus dari pending_acks → hapus dari Redis
```

### Jaminan At-Least-Once
- Pesan yang di-consume tapi belum di-ack masuk ke `pending_acks`
- Redelivery loop (setiap 5 detik) mengembalikan pesan yang melebihi `ACK_TIMEOUT` ke depan queue
- Pesan bisa terkirim lebih dari sekali jika consumer crash sebelum ack

### Endpoint
| Method | Path | Deskripsi |
|---|---|---|
| POST | `/queue/push` | Kirim pesan ke topik |
| POST | `/queue/consume` | Ambil pesan dari topik |
| POST | `/queue/ack` | Konfirmasi pesan sudah diproses |
| GET | `/queue/status` | Status antrean dan topik |
| GET | `/queue/ring` | Visualisasi hash ring |

---

## 4. MESI Cache Coherence (`src/nodes/cache_node.py`)

### Tujuan
Cache terdistribusi yang memastikan konsistensi data antar node menggunakan protokol MESI.

### State MESI
| State | Singkatan | Arti |
|---|---|---|
| Modified | M | Data dimodifikasi secara lokal; satu-satunya salinan yang valid |
| Exclusive | E | Data bersih; hanya ada di cache ini |
| Shared | S | Data bersih; bisa ada di cache lain juga |
| Invalid | I | Data tidak valid/tidak ada |

### Transisi State
```
                    BusRd dari peer
    E ─────────────────────────────────────► S
    │                                        │
    │ Write lokal                            │ Write lokal
    ▼                                        ▼
    M ◄──────────────────────────────────── M
    │  BusRdX dari peer (invalidasi)
    │
    ▼
    S ──── Invalidasi dari peer ────────────► I
    │
    │ Write lokal (BusRdX ke peer dulu)
    ▼
    M

Read miss (tidak ada peer):  I → E
Read miss (ada peer):        I → S
Write (apapun):              * → M  (setelah invalidasi peer)
```

### Alur Read
```
GET /cache/{key}
    ↓
[Cache lokal ada dan bukan Invalid?]
    Ya → return HIT, update LRU
    Tidak → BusRd: kirim POST /cache/snoop/read ke semua peer
         ↓
    [Ada peer yang punya?]
         Ya → insert sebagai Shared, return miss_peer
         Tidak → baca dari Redis (backing store)
              ↓
         [Ada di Redis?]
              Ya → insert sebagai Exclusive, return miss_store
              Tidak → return not_found
```

### Alur Write
```
PUT /cache/{key}
    ↓
BusRdX: kirim POST /cache/snoop/invalidate ke semua peer
    ↓
Update/insert lokal sebagai Modified
    ↓
Write-back ke Redis terjadi saat: eviction LRU atau shutdown
```

### LRU Replacement
- Menggunakan `OrderedDict` untuk melacak urutan akses
- Saat kapasitas penuh, entry paling lama tidak diakses dieviksi
- Entry Modified yang dieviksi di-flush ke Redis terlebih dahulu

### Endpoint
| Method | Path | Deskripsi |
|---|---|---|
| GET | `/cache/{key}` | Baca dengan protokol MESI |
| PUT | `/cache/{key}` | Tulis dengan protokol MESI |
| DELETE | `/cache/{key}` | Invalidasi entry |
| GET | `/cache-stats` | Statistik + distribusi state MESI |
| GET | `/cache-entries` | Semua entry dengan state |
| POST | `/cache/snoop/read` | Snoop BusRd (internal) |
| POST | `/cache/snoop/invalidate` | Snoop BusRdX (internal) |

---

## Lapisan Komunikasi

### Message Passing (`src/communication/message_passing.py`)
- HTTP client async menggunakan `httpx.AsyncClient`
- Setiap request inter-node menyertakan header `X-Node-ID` dan `X-Node-Secret`
- Retry logic: 3 kali percobaan dengan exponential backoff (base 100ms)
- Timeout: 5 detik per request

### Failure Detector (`src/communication/failure_detector.py`)
- Health check ke setiap peer setiap 2 detik
- Status peer: `UNKNOWN → ALIVE → SUSPECTED → DEAD`
- Node ditandai DEAD setelah 3 kegagalan berturut-turut
- Ketika node DEAD: Queue menghapusnya dari hash ring

---

## Keamanan (`src/utils/security.py`)

### Autentikasi
| Tipe | Header | Digunakan untuk |
|---|---|---|
| API Key biasa | `X-API-Key` | Request dari client eksternal |
| Admin Key | `X-API-Key` | Role admin (hapus, audit) |
| Node Secret | `X-Node-Secret` | Request antar-node (Raft, Cache Snoop) |

### Role-Based Access Control (RBAC)
| Role | Permissions |
|---|---|
| `reader` | GET saja |
| `writer` | GET, POST, PUT |
| `admin` | GET, POST, PUT, DELETE |

### Path Khusus
- **Public** (tanpa auth): `/health`, `/docs`, `/openapi.json`, `/redoc`
- **Internal** (hanya node secret): `/raft/*`, `/cache/snoop/*`

---

## Konfigurasi (`src/utils/config.py`)

Semua konfigurasi di-load dari environment variables menggunakan Pydantic `BaseSettings`:

```
NODE_ID, NODE_HOST, NODE_PORT   — Identitas node
PEER_NODES                       — URL peer dipisahkan koma
REDIS_URL                        — Koneksi Redis
API_KEY, ADMIN_KEY, NODE_SECRET  — Keamanan
ELECTION_TIMEOUT_MIN/MAX         — Timing Raft (ms)
HEARTBEAT_INTERVAL               — Interval heartbeat (ms)
CACHE_MAX_SIZE, CACHE_TTL        — Konfigurasi cache
QUEUE_ACK_TIMEOUT                — Batas waktu ack antrean (detik)
QUEUE_VIRTUAL_NODES              — Jumlah virtual node hash ring
```
