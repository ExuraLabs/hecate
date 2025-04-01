# Hecate 🔮 <img align="right" width="150" height="150" src=".github/assets/hecate_logo.png">

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Mypy](https://img.shields.io/badge/types-mypy-blue.svg)](http://mypy-lang.org/)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

> The magical gateway between Ogmios and Exura for Cardano blockchain synchronization

## Overview

Hecate is the data relay service that powers Exura, connecting to the Cardano blockchain through Ogmios and efficiently fetching both historical and real-time blockchain data. Named after the Greek goddess of magic, crossroads, and keeper of keys, Hecate serves as the crucial bridge between the blockchain and Exura's processing logic. Hecate focuses exclusively on reliable data acquisition and transmission, leaving all business logic processing to Exura.

## Architecture Overview

```
┌─────────────┐      ┌─────────────┐      ┌───────────────────┐      ┌───────────┐
│             │      │             │      │      HECATE       │      │           │
│   CARDANO   │─────►│   OGMIOS    │─────►│                   │─────►│   EXURA   │
│    NODE     │      │  WebSockets │      │  ┌─────────────┐  │      │           │
└─────────────┘      └─────────────┘      │  │   Prefect   │  │      └───────────┘
                                          │  │    Flows    │  │            ▲
                                          │  └─────────────┘  │            │
                                          └───────────────────┘            │
                                                    │                      │
                                                    ▼                      │
                                          ┌───────────────────┐            │
                                          │                   │            │
                                          │       REDIS       │────────────┘
                                          │                   │
                                          └───────────────────┘
```

Hecate consists of:

1. **Ogmios Client** - Robust asynchronous client for the Ogmios WebSocket API
2. **Data Relay** - Efficiently forward blockchain data with minimal transformation
3. **Prefect Flows** - Orchestrate historical and real-time data fetching
4. **Redis Integration** - Buffer transactions for Exura to process

## Features

- ⚡ **Parallel Historical Fetching** - Efficiently fetch the entire blockchain history in parallel
- 🔄 **Real-time Data Relay** - Stay current with the latest blocks and relay them to Exura
- 🛡️ **Reorg Detection** - Detect chain reorganizations and handle them gracefully
- 📊 **Advanced Monitoring** - Track connection status, latency, and throughput metrics via Prefect
- 🧰 **Flexible Deployment** - Run as a standalone service with simple configuration

## Installation

### Prerequisites

- Python 3.12+
- uv (Python package manager)
- Redis
- Ogmios node access

### Setup

```bash
# Clone the repository
git clone https://github.com/ExuraLabs/hecate.git
cd hecate

# Install dependencies
uv venv
uv sync --all-groups

# Set up environment variables
cp .env.example .env
# Edit .env with your configuration

# Set up pre-commit hooks
uv run pre-commit install
```

## Configuration

Hecate can be configured through environment variables or a `.env` file:

```
# Ogmios
OGMIOS_WS_URL=ws://localhost:1337

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# Prefect 
PREFECT_API_URL=http://127.0.0.1:4200/api
PREFECT_API_KEY=your_api_key

# Processing
HISTORICAL_BATCH_SIZE=1000
PARALLEL_WORKERS=4
CONFIRMATION_DEPTH=3
```

## Usage

### Development Environment

```bash
# Run tests
uv run pytest

# Start a Prefect agent locally
uv run prefect agent start -p default-agent-pool
```

### Starting Flows

```bash
# Deploy flows
uv run python -m hecate.deploy

# Run historical sync
uv run python -m hecate.cli historical --start-epoch 300 --end-epoch 310

# Run real-time sync
uv run python -m hecate.cli realtime
```

## Project Structure

```
hecate/
├── client/           # Ogmios WebSocket client
├── models/           # Data models and type definitions
├── flows/            # Prefect flow definitions
│   ├── historical.py # Historical synchronization flows
│   └── realtime.py   # Real-time synchronization flows
├── utils/            # Helper utilities
├── scripts/          # Operational scripts
├── tests/            # Test suite
└── deploy/           # Deployment configuration
```

## Development

### Testing

```bash
# Run all tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=hecate

# Run specific test file
uv run pytest tests/test_client.py
```

### Type Checking and Linting

```bash
# Run mypy for type checking
uv run mypy

# Run ruff for linting
uv run ruff check

# Run ruff for formatting
uv run ruff format
```


## License

This project is licensed under the Apache 2.0 License - see the LICENSE file for details.

## Acknowledgements

- [Ogmios](https://github.com/input-output-hk/ogmios) - WebSocket client and server that enables applications to speak with Cardano nodes
- [Prefect](https://www.prefect.io/) - Workflow orchestration tool
- [Exura](https://github.com/ExuraLabs/exura) - Cardano DeFi dashboard and analytics platform
