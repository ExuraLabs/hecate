# Hecate Flows

Hecate uses [Prefect](https://www.prefect.io/) to orchestrate data flows and periodic tasks. This
document covers the available flows, their configuration options, and usage patterns.

## Flow Types

### Historical Synchronization Flow


The historical sync flow efficiently retrieves and processes past blockchain data, with integrated metrics monitoring at every relevant stage. The current logic ensures robust, resumable, and observable execution:

#### Execution

```bash
uv run python -m flows.historical --start-epoch=208 --end-epoch=210 --batch-size=1000
```

#### How It Works

- **Resumable & Concurrent**: The flow detects the last synced epoch and continues from there,
  processing multiple epochs in parallel using Prefect's `ProcessPoolTaskRunner`.
- **Per-Epoch Streams**: Each epoch's block data is written directly to a dedicated Redis stream (
  `epoch:{N}`). Ordering is guaranteed by construction — consumers read epoch streams in ascending
  order.
- **Batch Processing**: Epochs are processed in concurrent batches, each subdivided into block batches for optimal memory and performance.
- **Factory Pattern**: Historical blocks are created via a fast factory, bypassing unnecessary validation for speed.
- **Backpressure**: Before each concurrent batch, the flow checks how many epoch streams are
  unconsumed in Redis. If the gap exceeds `REDIS_MAX_UNCONSUMED_EPOCHS`, it pauses until consumers
  catch up.
- **Stream Cleanup**: A background task iterates consumed epoch streams in ascending order and
  DELetes those where all consumer groups have acknowledged every entry, advancing `low_watermark`.
- **Error Handling**: Errors in epoch or batch sync are logged, and the flow continues with the next batch, preventing total blockage.
- **Checkpointing & Recovery**: Progress state is saved in Redis, allowing resume from the last successful point in case of failure.

#### Redis Integration & State Management

- **Redis** manages:
  - Last synced epoch
  - Resume maps per epoch
  - Per-epoch data streams and control events
- **Key Structure** (default prefix `hecate:history:`):
    - `<prefix>epoch:{N}`: Per-epoch Redis stream containing block-batch payloads
    - `<prefix>last_synced_epoch`: Highest sequentially completed epoch
    - `<prefix>low_watermark`: Lowest epoch whose stream still exists in Redis
    - `<prefix>ready_set`: Set of completed epochs awaiting sequential commit
    - `<prefix>resume_map`: Hash of in-progress epochs' resume positions
    - `<prefix>event_stream`: Stream of audit/control events (e.g. `epoch_complete`)

#### Relevant Configuration

- `start_epoch`: Initial epoch to sync
- `end_epoch`: Optional upper-bound epoch (inclusive); defaults to system checkpoint
- `batch_size`: Block batch size per epoch
- `concurrent_epochs`: Number of epochs to process in parallel (defaults to CPU count)
- `REDIS_MAX_UNCONSUMED_EPOCHS`: Backpressure threshold (default: 10)

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
- `end_epoch`: Optional upper-bound epoch (inclusive); defaults to system checkpoint
- `batch_size`: Number of blocks to process in each batch (default: 500)
- `concurrent_epochs`: CPU cores to utilize for parallel epoch processing (default: available cores)

```
