"""Randomized schedule exploration with replay and counterexample shrinking.

A schedule is the list of choice indices the scheduler drew. Replaying the
same list reproduces the same run exactly, so a failure can be shrunk by
deleting and lowering choices while it still fails the same way.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from pbench.model import Sim, Violation


@dataclass
class Result:
    choices: List[int]
    violation: Optional[Violation] = None
    trace: List[str] = field(default_factory=list)

    @property
    def kind(self) -> Optional[str]:
        return self.violation.kind if self.violation else None


def run(
    impl: str,
    seed: Optional[int] = None,
    prefix: Optional[List[int]] = None,
    max_steps: int = 300,
    **config: Any,
) -> Result:
    """Run one schedule: random from ``seed``, or a replay of ``prefix``."""
    rng = random.Random(seed)
    drawn: List[int] = []

    def choose(n: int) -> int:
        if prefix is not None:
            value = prefix[len(drawn)] % n
        else:
            value = rng.randrange(n)
        drawn.append(value)
        return value

    sim = Sim(impl, choose, **config)
    steps = max_steps if prefix is None else len(prefix)
    try:
        sim.check_safety()
        for _ in range(steps):
            acts = sim.actions()
            if not acts:
                break
            name, act = acts[choose(len(acts))]
            sim.do(name, act)
        sim.quiesce()
    except Violation as v:
        return Result(drawn, v, sim.trace)
    return Result(drawn, None, sim.trace)


def shrink(impl: str, failing: Result, **config: Any) -> Result:
    """Greedily minimize a failing schedule, keeping the violation kind."""
    best = failing

    def attempt(candidate: List[int]) -> bool:
        nonlocal best
        result = run(impl, prefix=candidate, **config)
        if result.kind == best.kind and len(result.choices) <= len(best.choices):
            if result.choices != best.choices:
                best = result
                return True
        return False

    improved = True
    while improved:
        improved = False
        for size in (16, 8, 4, 2, 1):
            i = 0
            while i < len(best.choices):
                if attempt(best.choices[:i] + best.choices[i + size :]):
                    improved = True
                else:
                    i += 1
        for i in range(len(best.choices)):
            if best.choices[i] > 0:
                lowered = best.choices[:i] + [0] + best.choices[i + 1 :]
                improved |= attempt(lowered)
    return best


def search(impl: str, seeds: int, **config: Any) -> Dict[str, Any]:
    """Run ``seeds`` schedules; count failures and shrink one of each kind."""
    counts: Dict[str, int] = {}
    first: Dict[str, Tuple[int, Result]] = {}
    for seed in range(seeds):
        result = run(impl, seed=seed, **config)
        if result.violation:
            counts[result.kind] = counts.get(result.kind, 0) + 1
            first.setdefault(result.kind, (seed, result))
    examples = {
        kind: (seed, len(result.choices), shrink(impl, result, **config))
        for kind, (seed, result) in sorted(first.items(), reverse=True)
    }
    return {"impl": impl, "seeds": seeds, "failures": counts, "examples": examples}
