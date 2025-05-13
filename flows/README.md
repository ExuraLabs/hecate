# Flow Documentation

## Update for main README.md

```markdown
## Prefect Flows 🔄

Hecate uses [Prefect](https://www.prefect.io/) to orchestrate both historical and real-time data
flows:

- **Historical Sync**: Efficiently backfill blockchain history in a resumable, concurrent manner
- **Periodic Tasks**: Automatically update epoch metadata and other system constants
- **[See detailed flows documentation](flows/README.md)**

```

## flows/README.md

```markdown
# Hecate Flows

Hecate uses [Prefect](https://www.prefect.io/) to orchestrate data flows and periodic tasks. This
document covers the available flows, their configuration options, and usage patterns.

## Flow Types

### Historical Synchronization Flow

The historical sync flow efficiently retrieves and processes past blockchain data:

```bash
# Run historical sync from a specific epoch to the current checkpoint
uv run python -m flows.historical --start-epoch=208 --batch-size=1000
```

#### Features

- **Resumable Processing**: Automatically continues from the last synced epoch
- **Concurrent Execution**: Processes multiple epochs in parallel using Dask
- **Memory Efficient**: Streams data in configurable batches to control memory usage
- **Failure Recovery**: Retries failed tasks and maintains sync progress
- **Multi-level Checkpointing**:
    - Tracks last successful block height per epoch
    - Manages completion status across epochs
    - Ensures data consistency through atomic operations

#### Redis Integration

The flow uses Redis for state management and data delivery:

- **Connection**: Uses `REDIS_URL` environment variable (defaults to `redis://localhost:6379/0`)
- **Key Structure**:
    - `<prefix>last_synced_epoch`: Highest sequentially completed epoch
    - `<prefix>ready_set`: Set of completed epochs awaiting sequential commit
    - `<prefix>resume_map`: Hash of in-progress epochs' resume positions
    - `<prefix>data_stream`: Stream of block-batch payloads
    - `<prefix>event_stream`: Stream of audit/control events

Default prefix is `hecate:history:` and can be configured when initializing the sink.

### Periodic Flow

The periodic flow automatically updates epoch boundaries, block counts, and other system constants:

```bash
# Run periodic flow to update epoch data
uv run python -m flows.periodic
```

#### Features

- **Automated Updates**: Fetches new epoch data from Koios and AdaStat APIs
- **Data Persistence**: Updates local CSV files with new information
- **Git Integration**: Optionally commits and pushes changes to a designated branch
- **Self-Managing**: Only updates when new epochs are available

#### Configuration Options

- `GITHUB_TOKEN`: Environment variable for GitHub authentication
- Updates are tracked in `flows/checkpoint.json`

## Flow Architecture

Both flows leverage Prefect's task-based execution model:

1. **Tasks**: Atomic units of work with defined inputs, outputs, and error handling
2. **Flow**: Orchestrates tasks, managing dependencies and execution order
3. **State**: Tracks progress and enables resumability on failure

#### Configuration Options

- `start_epoch`: First epoch to sync (defaults to first Shelley epoch)
- `batch_size`: Number of blocks to process in each batch (default: 1000)
- `n_workers`: CPU cores to utilize (default: available cores)
- `memory_limit`: Memory limit per worker (default: 2GB)

```
