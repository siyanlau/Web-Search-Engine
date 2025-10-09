
"""
bench_search.py

Quick-and-dirty benchmark for the v0.4 Searcher (blocked postings).
Measures latency for Boolean AND/OR and BM25 (topk) on random queries.

By default, queries are sampled from the lexicon terms and formed as 2-term queries.
You can also pass a file with one query per line.

Run examples:
  python bench_search.py
  python bench_search.py --queries queries.txt --mode bm25 --topk 10
"""

import argparse
import random
import time
import statistics

try:
    from engine.paths import LEXICON_PATH, POSTINGS_PATH, DOC_LENGTHS_PATH
    from engine.lexicon import Lexicon
    from engine.searcher import Searcher
except Exception as e:
    print("Import error. Run from project root so `engine` is importable.")
    raise

def load_queries(path):
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def sample_queries(lex, n=100, terms_per_q=2):
    terms = list(lex.keys())
    random.seed(1234)
    queries = []
    for _ in range(n):
        qs = random.sample(terms, terms_per_q)
        queries.append(" ".join(qs))
    return queries

def bench(searcher, queries, mode, topk):
    times = []
    count = 0
    for q in queries:
        t0 = time.perf_counter()
        if mode in ("and", "or"):
            _ = searcher.search(q, mode=mode.upper())
        else:
            _ = searcher.search(q, topk=topk)
        dt = (time.perf_counter() - t0) * 1000  # ms
        times.append(dt)
        count += 1
    return {
        "n": count,
        "avg_ms": statistics.mean(times),
        "p50_ms": statistics.median(times),
        "p95_ms": statistics.quantiles(times, n=20)[18] if len(times) >= 20 else max(times),
        "max_ms": max(times),
    }

def main(args):
    lex = Lexicon.load(LEXICON_PATH).map
    if args.queries:
        queries = load_queries(args.queries)
    else:
        queries = sample_queries(lex, n=args.num_queries, terms_per_q=args.terms_per_query)

    # Initialize searcher. If mode is bm25, keep doc_lengths; else force boolean.
    if args.mode == "bm25":
        s = Searcher(lexicon_path=LEXICON_PATH, postings_path=POSTINGS_PATH, doc_lengths=DOC_LENGTHS_PATH)
    else:
        s = Searcher(lexicon_path=LEXICON_PATH, postings_path=POSTINGS_PATH, doc_lengths=None)

    stats = bench(s, queries, mode=args.mode, topk=args.topk)
    print(f"Mode={args.mode.upper()}  Queries={stats['n']}  "
          f"avg={stats['avg_ms']:.2f}ms  p50={stats['p50_ms']:.2f}ms  "
          f"p95={stats['p95_ms']:.2f}ms  max={stats['max_ms']:.2f}ms")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", type=str, default=None, help="file with one query per line")
    ap.add_argument("--mode", type=str, default="bm25", choices=["and", "or", "bm25"], help="benchmark mode")
    ap.add_argument("--topk", type=int, default=10, help="top-k for BM25 mode")
    ap.add_argument("--num-queries", type=int, default=200, help="number of sampled queries if --queries not provided")
    ap.add_argument("--terms-per-query", type=int, default=2, help="how many terms per sampled query")
    args = ap.parse_args()
    main(args)
