import random
import uuid
from locust import HttpUser, task, between

class SyncSystemUser(HttpUser):
    # Tunggu antara 0.5 hingga 2 detik antar tugas
    wait_time = between(0.5, 2.0)
    
    def on_start(self):
        """Dijalankan saat setiap virtual user mulai"""
        # Set API Key default untuk semua request
        self.client.headers.update({"X-API-Key": "dev-api-key-123", "Content-Type": "application/json"})
        self.client_id = f"locust-client-{uuid.uuid4().hex[:8]}"

    @task(3)
    def cache_read_write(self):
        """Simulasi operasi baca dan tulis ke MESI Cache (Bobot 3)"""
        # Pilih key acak untuk simulasi cache hit/miss
        key = f"user:{random.randint(1, 100)}"
        
        # 30% kemungkinan write, 70% kemungkinan read
        if random.random() < 0.3:
            self.client.put(f"/cache/{key}", json={"value": f"data_for_{key}"}, name="/cache/[key] (PUT)")
        else:
            self.client.get(f"/cache/{key}", name="/cache/[key] (GET)")

    @task(2)
    def queue_operations(self):
        """Simulasi antrean terdistribusi: Push dan Consume (Bobot 2)"""
        topic = random.choice(["orders", "logs", "notifications"])
        
        # Push message
        self.client.post("/queue/push", json={
            "topic": topic,
            "data": {"event": "test_event", "value": random.randint(1, 1000)},
            "producer_id": self.client_id
        }, name="/queue/push")
        
        # Coba consume message
        response = self.client.post("/queue/consume", json={
            "topic": topic,
            "consumer_id": self.client_id
        }, name="/queue/consume")
        
        # Jika ada message, langsung ack
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "consumed" and "message_id" in data:
                self.client.post("/queue/ack", json={
                    "message_id": data["message_id"],
                    "consumer_id": self.client_id
                }, name="/queue/ack")

    @task(1)
    def lock_operations(self):
        """Simulasi akuisisi dan rilis Distributed Lock (Bobot 1)"""
        resource = random.choice(["db_table", "file_system", "config"])
        lock_type = random.choice(["shared", "exclusive"])
        
        # Acquire Lock
        response = self.client.post("/lock/acquire", json={
            "resource": resource,
            "client_id": self.client_id,
            "lock_type": lock_type,
            "ttl": 10
        }, name="/lock/acquire")
        
        # Jika berhasil dapat kunci, lepaskan
        if response.status_code == 200 and response.json().get("status") == "granted":
            self.client.post("/lock/release", json={
                "resource": resource,
                "client_id": self.client_id
            }, name="/lock/release")

    @task(1)
    def system_health(self):
        """Simulasi pengecekan status node dan cluster (Bobot 1)"""
        self.client.get("/health", name="/health")
