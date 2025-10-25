# Hecate Flows

Hecate uses [Prefect](https://www.prefect.io/) to orchestrate data flows and periodic tasks. This
document covers the available flows, their configuration options, and usage patterns.

## Flow Types

### Historical Synchronization Flow


The historical sync flow efficiently retrieves and processes past blockchain data, with integrated metrics monitoring at every relevant stage. The current logic ensures robust, resumable, and observable execution:

#### Execution

```bash
uv run python -m flows.historical --start-epoch=208 --batch-size=1000
```

#### How It Works (2025)

- **Resumable & Concurrent**: The flow detects the last synced epoch and continues from there, processing multiple epochs in parallel using Prefect and Dask.
- **Batch Processing**: Epochs are processed in concurrent batches, each subdivided into block batches for optimal memory and performance.
- **Factory Pattern**: Historical blocks are created via a fast factory, bypassing unnecessary validation for speed.
- **Integrated Metrics**: Metrics collection and publishing occur:
  - At the start of the flow
  - At the start of each batch of epochs
  - At the end of the flow
  This ensures periodic visibility, synchronized with actual progress and batch advancement.
- **Error Handling**: Errors in epoch or batch sync are logged, and the flow continues with the next batch, preventing total blockage.
- **Checkpointing & Recovery**: Progress state is saved in Redis, allowing resume from the last successful point in case of failure.

#### Redis Integration & State Management

- **Redis** manages:
  - Last synced epoch
  - Resume maps per epoch
  - Data and event streams
- **Key Structure**:
    - `<prefix>last_synced_epoch`: Highest sequentially completed epoch
    - `<prefix>ready_set`: Set of completed epochs awaiting sequential commit
    - `<prefix>resume_map`: Hash of in-progress epochs' resume positions
    - `<prefix>data_stream`: Stream of block-batch payloads
    - `<prefix>event_stream`: Stream of audit/control events
- **Configurable prefixes** isolate state for each run. Default prefix is `hecate:history:` and can be configured when initializing the sink.

#### Relevant Configuration

- `start_epoch`: Initial epoch to sync
- `batch_size`: Block batch size per epoch
- `n_workers`: Dask concurrency core
- Environment variables for Redis and Dask (see settings)

#### Example Execution Cycle

1. Initial metrics are collected.
2. The range of epochs to process is determined.
3. For each batch of epochs:
   - Metrics are collected.
   - Epochs are processed concurrently.
   - Progress and errors are logged.
4. On completion, final metrics are collected and total time is logged.

---

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
