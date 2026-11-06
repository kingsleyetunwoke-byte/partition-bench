# partition-bench

Schedule exploration for distributed partition-assignment protocols: find
interleavings that break exclusive ownership, replay them deterministically,
and shrink them to a minimal counterexample a person can read.

It grew out of work on the `SetPartitioner` recipe in
[kazoo](https://github.com/python-zk/kazoo), the Python ZooKeeper client:
adding pluggable assignment strategies (round-robin, contiguous) and the
fencing needed to make rebalancing safe. Bugs in this kind of code show up
only under rare orderings of lock returns, watch callbacks and session
expiry, so they slip past normal tests and code review. This project models
the protocol directly so those orderings can be searched instead of hoped for.

## What it models

- **Coordinator**: ZooKeeper's role. Party membership as sessions, and
  per-partition locks owned by `(session, lock-object)` pairs, as lock znodes
  are. Expired sessions lose their locks.
- **Workers**: each allocation attempt is a generator. Every `yield` is a
  point where the scheduler may run another attempt, deliver a membership
  callback, disconnect a worker, expire its session, reconnect it, or add a
  member.
- **Receipt vs delivery**: a membership change is *received* (flag set)
  before its callback is *delivered*. Most real races live in that gap.

Invariants:

- **Safety**, checked after every step: a partition is processed by at most
  one worker, and only while that worker's session holds its lock.
- **Liveness**, checked after healing (all reconnected, all callbacks
  delivered): workers settle on exactly the strategy's assignment.

## Results (2000 random schedules each, contiguous strategy)

| Implementation | Failed | By kind | Minimal counterexample (choices) |
|---|---|---|---|
| `naive` | 1164/2000 | safety 765, liveness 399 | safety 2, liveness 3 |
| `fenced` | 0/2000 | — | — |
| `fenced-no-disconnect-fence` | 0/2000 | — | — |
| `fenced-no-pending-check` | 0/2000 | — | — |
| `fenced-no-wait-check` | 1045/2000 | liveness 1045 | 2 |
| `fenced-no-connection-checks` | 16/2000 | safety 16 | 13 |

What the ablations show:

1. **Tentative locks must be private to an attempt.** With one shared lock
   list, a stale attempt returning late from a blocking acquire rolls back
   the *newer* attempt's locks, and the newer attempt then publishes
   partitions it no longer holds. Found in 2 scheduler choices.
2. **Waiting attempts must check staleness.** Otherwise an attempt planned
   from an old membership snapshot waits forever on a partition that now
   belongs to someone else, while holding locks the current attempt needs:
   a deadlock.
3. **The commit needs a connectivity check, atomic with publishing.**
   Without it, a worker that has lost its session publishes ownership. This
   failed in only 0.8% of random schedules, which is exactly the kind of bug
   that survives code review and normal testing. The shrinker reduces it to a
   13-step trace.
4. **Some guards are redundant here.** Fencing on disconnect and checking
   received-but-undelivered notifications never mattered in this model, once
   the commit checks connectivity atomically. In the real client they still
   matter, because connection state and callbacks are not atomic with the
   commit unless a lock is added to make them so.

## Usage

```
python3 -m pbench                         # naive vs fenced, 500 seeds, traces
python3 -m pbench --impl fenced-no-connection-checks --seeds 2000
python3 -m pbench --strategy round_robin --quiet
python3 -m unittest -v
```

Every reported failure prints a `prefix` that replays it exactly:

```python
from pbench.explore import run
print("\n".join(run("naive", prefix=[1, 1]).trace))
```

Pure Python 3.9+, no dependencies, no ZooKeeper needed.

## Layout

- `pbench/model.py`: coordinator, strategies, worker implementations, invariants
- `pbench/explore.py`: seeded runs, replay, greedy shrinking (delete blocks, lower choices)
- `pbench/__main__.py`: CLI
- `tests/`: strategy, determinism, and per-ablation regression tests

## Limitations and next steps

- Random search, not exhaustive. Small-scope bounded model checking with
  partial-order reduction would make "0 failures" a proof for the bound.
- The model is hand-written, so it can drift from the real code. The next
  step is driving the real kazoo recipe through a controllable fake
  handler, so the same schedules exercise production code.
- Use as an evaluation harness: give a coding agent the naive
  implementation and score its patch against the explorer rather than a
  fixed test suite. Fixed suites are easy to overfit, and an adversarial
  scheduler is not.
