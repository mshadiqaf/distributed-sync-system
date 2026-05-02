# Distributed Synchronization System

A distributed synchronization system implementing core distributed systems concepts including consensus, distributed locking, message queuing, and cache coherence.

## Overview

This project demonstrates distributed systems principles through a multi-node architecture built with Python and FastAPI. The system features:

- **Distributed Lock Manager** — Raft-based consensus for distributed locking
- **Distributed Queue** — Consistent hashing for message distribution
- **Cache Coherence** — MESI protocol for cache consistency
- **Containerized Deployment** — Docker-based multi-node orchestration

## Tech Stack

- Python 3.12+
- FastAPI (async-first)
- Redis (distributed state)
- Docker & Docker Compose

## Quick Start

```bash
# Clone the repository
git clone https://github.com/mshadiqaf/distributed-sync-system.git
cd distributed-sync-system

# Install dependencies
pip install -r requirements.txt

# Copy environment configuration
cp .env.example .env

# Start the system
# (Full instructions will be added as the project develops)
```

## Project Status

🚧 Under active development

## License

MIT
