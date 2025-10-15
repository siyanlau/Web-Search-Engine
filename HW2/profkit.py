
# profkit.py — ultra‑light profiling helpers for the merger pipeline
# Drop this file next to your engine/ package and `from profkit import timeit, tick, COUNTERS`.
# Toggle via env var: set PROFKIT=1 to enable; otherwise it's no‑op with near‑zero overhead.

import os
import time
from collections import defaultdict, OrderedDict
from contextlib import contextmanager

ENABLED = os.getenv("PROFKIT", "0") == "1"
COUNTERS = defaultdict(float)  # str -> float (counts / milliseconds / bytes etc.)

def tick(name: str, n: float = 1.0):
    if ENABLED:
        COUNTERS[name] += n

@contextmanager
def timeit(name: str):
    if not ENABLED:
        yield
        return
    t0 = time.perf_counter()
    try:
        yield
    finally:
        COUNTERS[name] += (time.perf_counter() - t0) * 1000.0  # ms

# A tiny LRU you can use to cache decoded blocks if you want to test caching impact quickly.
class LRU:
    def __init__(self, capacity=32):
        self.capacity = capacity
        self._d = OrderedDict()

    def get(self, key):
        if key in self._d:
            v = self._d.pop(key)
            self._d[key] = v
            return v
        return None

    def put(self, key, value):
        if key in self._d:
            self._d.pop(key)
        elif len(self._d) >= self.capacity:
            self._d.popitem(last=False)
        self._d[key] = value
