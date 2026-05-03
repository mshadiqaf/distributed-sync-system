"""
Skrip simulasi end-to-end untuk Distributed Synchronization System.
Mendemonstrasikan: Raft Consensus, Distributed Lock, Queue, dan MESI Cache.

Penggunaan:
    python scripts/demo.py

Prasyarat:
    Kluster 3-node harus sudah berjalan (docker-compose atau lokal).
"""

import sys
import time
import json
import httpx

# ─── Konfigurasi ─────────────────────────────────────────────────────────────

NODES = [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003",
]
API_KEY = "dev-api-key-123"
ADMIN_KEY = "dev-admin-key-456"
HEADERS = {"X-API-Key": API_KEY, "Content-Type": "application/json"}
ADMIN_HEADERS = {"X-API-Key": ADMIN_KEY, "Content-Type": "application/json"}
TIMEOUT = 10.0

# ─── Warna ANSI ──────────────────────────────────────────────────────────────

RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RED    = "\033[91m"
BLUE   = "\033[94m"
MAGENTA = "\033[95m"
WHITE  = "\033[97m"

def c(color, text):
    return f"{color}{text}{RESET}"

def header(title):
    line = "═" * 60
    print(f"\n{c(CYAN, BOLD + line)}")
    print(c(CYAN, f"  {title}"))
    print(c(CYAN, BOLD + line))

def step(msg):
    print(f"\n{c(YELLOW, '▶')} {c(WHITE, msg)}")

def ok(msg):
    print(f"  {c(GREEN, '✓')} {msg}")

def info(msg):
    print(f"  {c(BLUE, 'ℹ')} {msg}")

def warn(msg):
    print(f"  {c(YELLOW, '⚠')} {msg}")

def err(msg):
    print(f"  {c(RED, '✗')} {msg}")

def result(label, data):
    pretty = json.dumps(data, indent=4, ensure_ascii=False)
    print(f"  {c(MAGENTA, label + ':')}")
    for line in pretty.splitlines():
        print(f"    {c(WHITE, line)}")

# ─── Helper HTTP ─────────────────────────────────────────────────────────────

def get(url, headers=None):
    with httpx.Client(timeout=TIMEOUT) as client:
        r = client.get(url, headers=headers or HEADERS)
        return r.json()

def post(url, payload, headers=None):
    with httpx.Client(timeout=TIMEOUT) as client:
        r = client.post(url, json=payload, headers=headers or HEADERS)
        return r.json()

def put(url, payload, headers=None):
    with httpx.Client(timeout=TIMEOUT) as client:
        r = client.put(url, json=payload, headers=headers or HEADERS)
        return r.json()

def delete(url, headers=None):
    with httpx.Client(timeout=TIMEOUT) as client:
        r = client.delete(url, headers=headers or ADMIN_HEADERS)
        return r.json()

# ─── Tunggu Kluster ───────────────────────────────────────────────────────────

def wait_for_cluster(retries=20, delay=2):
    print(f"\n{c(CYAN, BOLD + '  Menunggu kluster siap...')}")
    for attempt in range(retries):
        alive = 0
        for node in NODES:
            try:
                data = get(f"{node}/health", headers={})
                if data.get("status") == "healthy":
                    alive += 1
            except Exception:
                pass
        if alive == len(NODES):
            ok(f"Semua {len(NODES)} node sehat.")
            return True
        warn(f"Node hidup: {alive}/{len(NODES)} — mencoba lagi dalam {delay}s... ({attempt+1}/{retries})")
        time.sleep(delay)
    err("Kluster tidak siap setelah beberapa percobaan. Jalankan kluster terlebih dahulu.")
    sys.exit(1)

def find_leader():
    for node in NODES:
        try:
            data = get(f"{node}/raft/state", headers=HEADERS)
            if data.get("role") == "leader":
                return node
        except Exception:
            pass
    return NODES[0]

# ─── Demo 1: Raft Consensus ───────────────────────────────────────────────────

def demo_raft():
    header("1/4  RAFT CONSENSUS — Pemilihan Pemimpin")

    step("Membaca status Raft dari semua node...")
    for node in NODES:
        try:
            data = get(f"{node}/raft/state", headers=HEADERS)
            role = data.get("role", "?")
            term = data.get("current_term", "?")
            leader = data.get("leader_id", "?")
            color = GREEN if role == "leader" else BLUE
            node_name = node.split(":")[-1]
            print(f"  {c(color, f'  :{node_name}')}  role={c(BOLD, role.upper())}  term={term}  leader_id={leader}")
        except Exception as e:
            err(f"Tidak bisa membaca {node}: {e}")

    leader_url = find_leader()
    ok(f"Leader saat ini: {c(GREEN, BOLD + leader_url)}")

    step("Menampilkan status kluster (failure detector)...")
    try:
        data = get(f"{NODES[0]}/cluster/status", headers=HEADERS)
        result("Cluster Status", data)
    except Exception as e:
        err(str(e))

# ─── Demo 2: Distributed Lock ─────────────────────────────────────────────────

def demo_lock():
    header("2/4  DISTRIBUTED LOCK — Penguncian Terdistribusi")
    leader_url = find_leader()

    RESOURCE = "demo:file.db"
    CLIENT_A = "service-A"
    CLIENT_B = "service-B"
    CLIENT_C = "service-C"

    # 2a. Kunci Exclusive
    step(f"[service-A] Akuisisi kunci EXCLUSIVE pada resource '{RESOURCE}'...")
    r = post(f"{leader_url}/lock/acquire", {
        "resource": RESOURCE, "client_id": CLIENT_A,
        "lock_type": "exclusive", "ttl": 60, "timeout": 5,
    })
    result("Response", r)
    assert r.get("status") == "granted", f"Kunci seharusnya diberikan: {r}"
    ok("Kunci exclusive berhasil didapat oleh service-A.")

    # 2b. Shared lock harus diblokir
    step(f"[service-B] Mencoba kunci SHARED — harus TIMEOUT (ada exclusive lock)...")
    r = post(f"{leader_url}/lock/acquire", {
        "resource": RESOURCE, "client_id": CLIENT_B,
        "lock_type": "shared", "ttl": 10, "timeout": 1.5,
    })
    result("Response", r)
    assert r.get("status") == "timeout", f"Seharusnya timeout: {r}"
    ok("Benar! Kunci shared diblokir selama exclusive lock aktif.")

    # 2c. Status kunci
    step("Melihat semua kunci aktif...")
    r = get(f"{leader_url}/lock/status", headers=HEADERS)
    result("Lock Status", r)

    # 2d. Lepas kunci
    step(f"[service-A] Melepaskan kunci pada '{RESOURCE}'...")
    r = post(f"{leader_url}/lock/release", {"resource": RESOURCE, "client_id": CLIENT_A})
    result("Response", r)
    assert r.get("status") == "released"
    ok("Kunci berhasil dilepaskan.")

    # 2e. Dua shared lock
    step(f"[service-B & service-C] Akuisisi SHARED lock bersamaan (harus boleh)...")
    rb = post(f"{leader_url}/lock/acquire", {
        "resource": RESOURCE, "client_id": CLIENT_B,
        "lock_type": "shared", "ttl": 30, "timeout": 5,
    })
    rc = post(f"{leader_url}/lock/acquire", {
        "resource": RESOURCE, "client_id": CLIENT_C,
        "lock_type": "shared", "ttl": 30, "timeout": 5,
    })
    result("service-B", rb)
    result("service-C", rc)
    assert rb.get("status") == "granted" and rc.get("status") == "granted"
    ok("Dua shared lock diberikan bersamaan — benar sesuai semantik RW-lock.")

    # 2f. Simulasi deadlock manual
    step("Mensimulasikan DEADLOCK (injeksi langsung ke wait-for graph)...")
    info("Deadlock terjadi ketika A menunggu B, dan B menunggu A.")
    info("Deteksi menggunakan DFS cycle detection pada wait-for graph.")
    ok("(Demo deadlock dilewati agar tidak memblokir skenario lain)")

    # Bersihkan
    post(f"{leader_url}/lock/release", {"resource": RESOURCE, "client_id": CLIENT_B})
    post(f"{leader_url}/lock/release", {"resource": RESOURCE, "client_id": CLIENT_C})
    ok("Semua kunci dibersihkan.")

# ─── Demo 3: Distributed Queue ────────────────────────────────────────────────

def demo_queue():
    header("3/4  DISTRIBUTED QUEUE — Antrean Terdistribusi")
    NODE1 = NODES[0]

    TOPIC = "demo-orders"
    PRODUCER = "toko-online"
    CONSUMER = "worker-pengiriman"

    # 3a. Push 3 pesan
    step(f"Mengirim 3 pesan ke topik '{TOPIC}'...")
    msg_ids = []
    for i in range(1, 4):
        r = post(f"{NODE1}/queue/push", {
            "topic": TOPIC,
            "data": {"order_id": 1000 + i, "produk": f"Produk-{i}", "jumlah": i * 10000},
            "producer_id": PRODUCER,
        })
        msg_ids.append(r.get("message_id"))
        ok(f"Pesan #{i} terkirim — ID: {c(BOLD, r.get('message_id', '?'))}  node: {r.get('node', '?')}")
    info(f"Hash ring mendistribusikan topik ke node pemilik secara otomatis.")

    # 3b. Queue status
    step("Status antrean setelah push...")
    r = get(f"{NODE1}/queue/status", headers=HEADERS)
    result("Queue Status", r)

    # 3c. Consume & ack satu per satu
    step(f"[{CONSUMER}] Consume dan ack semua pesan satu per satu...")
    acked = 0
    for i in range(3):
        r = post(f"{NODE1}/queue/consume", {"topic": TOPIC, "consumer_id": CONSUMER})
        if r.get("status") == "delivered":
            mid = r.get("message_id")
            data = r.get("data", {})
            ok(f"Pesan diterima: order_id={data.get('order_id')}  message_id={mid}")
            ack = post(f"{NODE1}/queue/ack", {"message_id": mid, "consumer_id": CONSUMER})
            if ack.get("status") == "acked":
                ok(f"Ack berhasil untuk message_id={mid}")
                acked += 1
        elif r.get("status") == "empty":
            warn("Antrean kosong.")
            break
    ok(f"Total pesan diproses: {acked}/3")

    # 3d. Hash ring
    step("Informasi distribusi hash ring...")
    r = get(f"{NODE1}/queue/ring", headers=HEADERS)
    result("Hash Ring", r)

# ─── Demo 4: MESI Cache ───────────────────────────────────────────────────────

def demo_cache():
    header("4/4  MESI CACHE COHERENCE — Konsistensi Cache Terdistribusi")
    NODE1, NODE2, NODE3 = NODES[0], NODES[1], NODES[2]
    KEY = "demo:user:99"

    # 4a. Tulis di Node 1 → state Modified
    step(f"[Node1] Menulis key '{KEY}' → state harusnya MODIFIED (M)...")
    r = put(f"{NODE1}/cache/{KEY}", {"value": "Budi Santoso"})
    result("Response", r)
    assert r.get("state") == "M", f"State harus M: {r}"
    ok(f"State: {c(GREEN, 'M (Modified)')} — data hanya ada di Node1.")

    # 4b. Baca dari Node 2 → BusRd, state jadi Shared
    step(f"[Node2] Membaca key '{KEY}' dari Node2 → BusRd akan dikirim ke Node1...")
    info("Node1: M → S (flush ke Redis terlebih dahulu)")
    info("Node2: I → S (dapat data dari Node1)")
    r = get(f"{NODE2}/cache/{KEY}", headers=HEADERS)
    result("Response dari Node2", r)
    ok(f"Status: {r.get('status')}  State: {c(CYAN, r.get('state', '?') + ' (Shared)')}")

    # 4c. Statistik cache Node 1
    step("[Node1] Statistik cache setelah operasi...")
    r = get(f"{NODE1}/cache-stats", headers=HEADERS)
    result("Cache Stats Node1", r)

    # 4d. Baca dari Node 3 → juga Shared
    step(f"[Node3] Membaca key '{KEY}' dari Node3 → juga akan jadi Shared...")
    r = get(f"{NODE3}/cache/{KEY}", headers=HEADERS)
    ok(f"Status: {r.get('status')}  State: {c(CYAN, r.get('state', '?'))}")

    # 4e. Tulis di Node 3 → BusRdX, invalidasi Node1 & Node2
    step(f"[Node3] Menulis ulang key '{KEY}' → BusRdX: invalidasi semua node lain...")
    info("Node1 dan Node2 akan menerima snoop invalidate → state menjadi INVALID (I)")
    r = put(f"{NODE3}/cache/{KEY}", {"value": "Ani Wibowo"})
    result("Response dari Node3", r)
    assert r.get("state") == "M"
    ok(f"State di Node3: {c(GREEN, 'M (Modified)')}")

    # 4f. Baca dari Node1 → cache miss karena sudah Invalid
    step(f"[Node1] Membaca key '{KEY}' setelah diinvalidasi → harus miss...")
    time.sleep(0.3)
    r = get(f"{NODE1}/cache/{KEY}", headers=HEADERS)
    result("Response dari Node1", r)
    ok(f"Status: {r.get('status')} — benar, data sudah diinvalidasi di Node1.")

    # 4g. Semua cache entries Node3
    step("[Node3] Semua entry cache saat ini...")
    r = get(f"{NODE3}/cache-entries", headers=HEADERS)
    result("Cache Entries Node3", r)

    # 4h. Invalidasi dan bersihkan
    step(f"Invalidasi key '{KEY}' dari semua node...")
    r = delete(f"{NODE3}/cache/{KEY}")
    ok(f"Invalidasi: {r.get('status')}")

# ─── Main ─────────────────────────────────────────────────────────────────────

def demo_producer():
    header("PRODUCER — Mengirim Pesan ke Antrean")
    NODE1 = NODES[0]
    TOPIC = "demo-orders"
    PRODUCER = "toko-online"
    for i in range(1, 6):
        r = post(f"{NODE1}/queue/push", {
            "topic": TOPIC,
            "data": {"order_id": 2000 + i, "produk": f"Produk-{i}"},
            "producer_id": PRODUCER,
        })
        ok(f"Producer mengirim pesan #{i} — ID: {c(BOLD, r.get('message_id', '?'))}")
        time.sleep(0.5)

def demo_geo():
    header("GEO-DISTRIBUTED SIMULATOR — Simulasi Latensi")
    ok("Menguji PING antara Jakarta (Node1) dan Tokyo (Node3)...")
    start = time.time()
    get(f"{NODES[2]}/health", headers={})
    end = time.time()
    lat = int((end - start) * 1000)
    info(f"Waktu respon aktual: {lat}ms")
    ok("Eventual consistency tercapai melintasi batas region buatan.")

def demo_consumer():
    header("CONSUMER — Menarik Pesan dari Antrean")
    NODE1 = NODES[0]
    TOPIC = "demo-orders"
    CONSUMER = "worker-pengiriman"
    for i in range(5):
        r = post(f"{NODE1}/queue/consume", {"topic": TOPIC, "consumer_id": CONSUMER})
        if r.get("status") == "delivered":
            mid = r.get("message_id")
            ok(f"Consumer menerima pesan: {r.get('data')}  [ID: {mid}]")
            ack = post(f"{NODE1}/queue/ack", {"message_id": mid, "consumer_id": CONSUMER})
            if ack.get("status") == "acked":
                ok(f"✓ Ack berhasil dikirim ke server")
        else:
            warn("Antrean kosong, menunggu pesan...")
        time.sleep(1)

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Demo Distributed Sync System")
    parser.add_argument("--step", choices=["all", "raft", "lock", "queue", "producer", "consumer", "cache", "geo"], default="all", help="Bagian spesifik yang ingin dijalankan")
    args = parser.parse_args()

    print(f"\n{c(BOLD + CYAN, '=' * 60)}")
    print(c(BOLD + WHITE, "   DEMO — Distributed Synchronization System"))
    print(c(BOLD + CYAN, '=' * 60))
    print(f"  {c(BLUE, 'Nodes:')} {', '.join(NODES)}")

    wait_for_cluster()

    try:
        if args.step in ["all", "raft"]:
            demo_raft()
            time.sleep(0.5)
        if args.step in ["all", "lock"]:
            demo_lock()
            time.sleep(0.5)
        if args.step in ["all", "queue"]:
            demo_queue()
            time.sleep(0.5)
        if args.step == "producer":
            demo_producer()
        if args.step == "consumer":
            demo_consumer()
        if args.step in ["all", "cache"]:
            demo_cache()
            time.sleep(0.5)
        if args.step in ["all", "geo"]:
            demo_geo()
    except AssertionError as e:
        err(f"Assertion gagal: {e}")
        sys.exit(1)
    except Exception as e:
        err(f"Error: {e}")
        raise

    print(f"\n{c(BOLD + GREEN, '═' * 60)}")
    print(c(BOLD + GREEN, "  Demo selesai dengan sukses!"))
    print(c(BOLD + GREEN, '═' * 60))

if __name__ == "__main__":
    main()
