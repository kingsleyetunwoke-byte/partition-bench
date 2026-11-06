"""A small executable model of workers sharing partitions through a coordinator.

The coordinator stands in for ZooKeeper: it keeps party membership as
sessions, and per-partition locks owned by (session, token) pairs, where a
token identifies one lock object, as a lock znode does in kazoo. A session
that expires loses its locks.

Workers run allocation attempts as generators. Every ``yield`` is a point
where the scheduler may run something else: another attempt, a delivered
membership notification, a disconnect or a session expiry. Two worker
implementations are provided:

``NaiveWorker``
    Mirrors a common first implementation: one lock list shared by every
    attempt, and a commit that only checks the attempt generation.

``FencedWorker``
    Keeps each attempt's tentative locks private, fences attempts on
    disconnect, and makes a final check of generation, connectivity and
    *received but undelivered* notifications before publishing a share.
"""

from __future__ import annotations

import itertools
from typing import Callable, Dict, Iterator, List, Optional, Tuple

Strategy = Callable[[List[str], List[int]], Dict[str, List[int]]]


def round_robin(members: List[str], partitions: List[int]) -> Dict[str, List[int]]:
    ms, ps = sorted(members), sorted(partitions)
    return {m: ps[i :: len(ms)] for i, m in enumerate(ms)}


def contiguous(members: List[str], partitions: List[int]) -> Dict[str, List[int]]:
    ms, ps = sorted(members), sorted(partitions)
    size, extra = divmod(len(ps), len(ms))
    shares, start = {}, 0
    for i, m in enumerate(ms):
        n = size + (1 if i < extra else 0)
        shares[m] = ps[start : start + n]
        start += n
    return shares


STRATEGIES: Dict[str, Strategy] = {
    "round_robin": round_robin,
    "contiguous": contiguous,
}


class Coordinator:
    """Membership and locks, as the server sees them."""

    def __init__(self) -> None:
        self.members: Dict[int, str] = {}
        self.locks: Dict[int, Tuple[int, int]] = {}
        self._sessions = itertools.count(1)

    def open_session(self, wid: str) -> int:
        session = next(self._sessions)
        self.members[session] = wid
        return session

    def expire(self, session: int) -> None:
        self.members.pop(session, None)
        for p, (owner, _) in list(self.locks.items()):
            if owner == session:
                del self.locks[p]

    def try_lock(self, p: int, session: int, token: int) -> bool:
        if session not in self.members:
            return False
        holder = self.locks.get(p)
        if holder is None:
            self.locks[p] = (session, token)
            return True
        return holder == (session, token)

    def unlock(self, p: int, session: int, token: int) -> None:
        if self.locks.get(p) == (session, token):
            del self.locks[p]


class Worker:
    """State and event handlers shared by both implementations."""

    def __init__(self, wid: str, sim: "Sim") -> None:
        self.wid = wid
        self.sim = sim
        self.session: Optional[int] = None
        self.connected = False
        self.gen = 0
        # Set when a membership change is received, cleared when its
        # callback is delivered. The gap between the two is where races live.
        self.notify_pending = False
        self.published: List[int] = []

    @property
    def coord(self) -> Coordinator:
        return self.sim.coord

    def release(self, p: int, token: int) -> bool:
        if not self.connected:
            return False
        self.coord.unlock(p, self.session, token)
        return True

    def connect(self) -> None:
        if self.session is None or self.session not in self.coord.members:
            self.session = self.sim.open_session(self)
        self.connected = True
        self.rebalance()

    def disconnect(self) -> None:
        self.connected = False
        self.published = []

    def deliver(self) -> None:
        self.notify_pending = False
        self.rebalance()

    def rebalance(self) -> None:
        self.release_all()
        self.published = []
        self.gen += 1
        self.sim.spawn(self, self.allocate(self.gen))

    def share_for(self, members: List[str], gen: int) -> List[int]:
        share = self.sim.strategy(members, self.sim.partitions)[self.wid]
        self.sim.log(f"{self.wid} g{gen} plans {share} from {members}")
        return share

    def release_all(self) -> None:
        raise NotImplementedError

    def allocate(self, gen: int) -> Iterator[None]:
        raise NotImplementedError


class NaiveWorker(Worker):
    def __init__(self, wid: str, sim: "Sim") -> None:
        super().__init__(wid, sim)
        self.held: List[Tuple[int, int]] = []

    def release_all(self) -> None:
        for p, token in list(self.held):
            if self.release(p, token):
                self.held.remove((p, token))

    def allocate(self, gen: int) -> Iterator[None]:
        members = self.sim.member_ids()
        if self.gen != gen or self.wid not in members:
            return
        share = self.share_for(members, gen)
        token = self.sim.new_token()
        for p in share:
            yield
            while not self.connected or not self.coord.try_lock(
                p, self.session, token
            ):
                yield
            self.sim.log(f"{self.wid} g{gen} locked p{p}")
            self.held.append((p, token))
        yield
        if self.gen != gen:
            # Releases the shared list, which may hold a newer attempt's locks.
            self.sim.log(f"{self.wid} g{gen} stale, releasing {len(self.held)} locks")
            self.release_all()
            return
        self.published = list(share)
        self.sim.log(f"{self.wid} g{gen} publishes {share}")


class FencedWorker(Worker):
    # Each guard can be switched off to measure whether it is needed.
    fence_on_disconnect = True
    check_pending = True
    check_while_waiting = True
    check_connected = True

    def __init__(self, wid: str, sim: "Sim") -> None:
        super().__init__(wid, sim)
        self.committed: List[Tuple[int, int]] = []
        self.orphans: List[Tuple[int, int]] = []

    def disconnect(self) -> None:
        super().disconnect()
        if self.fence_on_disconnect:
            self.gen += 1

    def release_all(self) -> None:
        for held in (self.committed, self.orphans):
            for p, token in list(held):
                if self.release(p, token):
                    held.remove((p, token))

    def allocate(self, gen: int) -> Iterator[None]:
        def stale() -> bool:
            return (
                self.gen != gen
                or (self.check_pending and self.notify_pending)
                or (self.check_connected and not self.connected)
            )

        members = self.sim.member_ids()
        if stale() or self.wid not in members:
            return
        share = self.share_for(members, gen)
        token = self.sim.new_token()
        pending: List[int] = []
        for p in share:
            yield
            while not (stale() and self.check_while_waiting) and not (
                self.connected and self.coord.try_lock(p, self.session, token)
            ):
                yield
            if stale():
                break
            self.sim.log(f"{self.wid} g{gen} locked p{p}")
            pending.append(p)
        else:
            yield
        # The final check and the commit run in one step, as they do under
        # the client's watch-event lock in the real recipe.
        if stale():
            for p in pending:
                if not self.release(p, token):
                    self.orphans.append((p, token))
            self.sim.log(f"{self.wid} g{gen} stale, rolled back its own {len(pending)} locks")
            return
        self.committed.extend((p, token) for p in pending)
        self.published = list(share)
        self.sim.log(f"{self.wid} g{gen} publishes {share}")


def _ablation(name: str, **guards: bool) -> type:
    return type(name, (FencedWorker,), guards)


IMPLEMENTATIONS = {
    "naive": NaiveWorker,
    "fenced": FencedWorker,
    "fenced-no-disconnect-fence": _ablation(
        "NoDisconnectFence", fence_on_disconnect=False
    ),
    "fenced-no-pending-check": _ablation("NoPendingCheck", check_pending=False),
    "fenced-no-wait-check": _ablation("NoWaitCheck", check_while_waiting=False),
    "fenced-no-connection-checks": _ablation(
        "NoConnectionChecks", fence_on_disconnect=False, check_connected=False
    ),
}


class Violation(Exception):
    def __init__(self, kind: str, message: str) -> None:
        super().__init__(f"{kind}: {message}")
        self.kind = kind


class Sim:
    """Runs one schedule, with choices drawn from ``choose(n) -> index``."""

    def __init__(
        self,
        impl: str,
        choose: Callable[[int], int],
        initial_workers: int = 3,
        max_workers: int = 4,
        partitions: int = 6,
        strategy: str = "contiguous",
        adversary_budget: int = 6,
    ) -> None:
        self.worker_cls = IMPLEMENTATIONS[impl]
        self.choose = choose
        self.coord = Coordinator()
        self.partitions = list(range(partitions))
        self.strategy = STRATEGIES[strategy]
        self.max_workers = max_workers
        self.adversary_budget = adversary_budget
        self.workers: List[Worker] = []
        self.tasks: List[Tuple[Worker, int, Iterator[None]]] = []
        self.trace: List[str] = []
        self._tokens = itertools.count(1)
        for _ in range(initial_workers):
            self._join()

    # -- hooks used by workers --------------------------------------------

    def log(self, line: str) -> None:
        self.trace.append(line)

    def new_token(self) -> int:
        return next(self._tokens)

    def member_ids(self) -> List[str]:
        return sorted(self.coord.members.values())

    def spawn(self, worker: Worker, task: Iterator[None]) -> None:
        self.tasks.append((worker, worker.gen, task))

    def open_session(self, worker: Worker) -> int:
        session = self.coord.open_session(worker.wid)
        self._membership_changed()
        return session

    def _membership_changed(self) -> None:
        for w in self.workers:
            if w.session in self.coord.members:
                w.notify_pending = True

    # -- actions ---------------------------------------------------------

    def _join(self) -> None:
        w = self.worker_cls(f"w{len(self.workers) + 1}", self)
        self.workers.append(w)
        self.log(f"{w.wid} joins")
        w.connect()

    def _run_task(self, index: int) -> None:
        worker, gen, task = self.tasks[index]
        try:
            next(task)
        except StopIteration:
            self.tasks.pop(index)

    def _expire(self, w: Worker) -> None:
        self.log(f"{w.wid} session expires")
        self.coord.expire(w.session)
        self._membership_changed()

    def actions(self) -> List[Tuple[str, Callable[[], None]]]:
        acts: List[Tuple[str, Callable[[], None]]] = [
            (f"step {w.wid} g{g}", lambda i=i: self._run_task(i))
            for i, (w, g, _) in enumerate(self.tasks)
        ]
        for w in self.workers:
            if w.connected and w.notify_pending:
                acts.append((f"deliver {w.wid}", w.deliver))
        if self.adversary_budget > 0:
            for w in self.workers:
                if w.connected:
                    acts.append((f"disconnect {w.wid}", w.disconnect))
                else:
                    acts.append((f"reconnect {w.wid}", w.connect))
                    if w.session in self.coord.members:
                        acts.append((f"expire {w.wid}", lambda w=w: self._expire(w)))
            if len(self.workers) < self.max_workers:
                acts.append(("join", self._join))
        return acts

    def do(self, name: str, act: Callable[[], None]) -> None:
        if not name.startswith("step"):
            self.log(f"-- {name}")
            if not name.startswith("deliver"):
                self.adversary_budget -= 1
        act()
        self.check_safety()

    # -- invariants --------------------------------------------------------

    def check_safety(self) -> None:
        owner: Dict[int, str] = {}
        for w in self.workers:
            for p in w.published:
                if p in owner:
                    raise Violation("safety", f"p{p} processed by {owner[p]} and {w.wid}")
                owner[p] = w.wid
                holder = self.coord.locks.get(p)
                if holder is None or holder[0] != w.session:
                    raise Violation(
                        "safety", f"{w.wid} processes p{p} without holding its lock"
                    )

    def converged(self) -> bool:
        members = self.member_ids()
        if not members:
            return False
        expected = self.strategy(members, self.partitions)
        return all(
            w.connected and not w.notify_pending and w.published == expected[w.wid]
            for w in self.workers
        )

    def quiesce(self, bound: int = 5000) -> None:
        """Heal the system and require that it settles on the right shares."""
        self.adversary_budget = 0
        for w in self.workers:
            if not w.connected:
                self.do(f"reconnect {w.wid}", w.connect)
        for _ in range(bound):
            for w in self.workers:
                if w.connected and w.notify_pending:
                    self.do(f"deliver {w.wid}", w.deliver)
            if self.converged():
                return
            for i in reversed(range(len(self.tasks))):
                self._run_task(i)
            self.check_safety()
        raise Violation("liveness", "workers never settled on the expected shares")
