# engine/parallel_merge.py
from __future__ import annotations
import os, math, uuid, argparse
from typing import List, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed

from engine.runio import RunReader, RunWriter
from engine.paths import POSTINGS_PATH, LEXICON_PATH
from engine.merger import merge_runs_to_index  # 你已有的合并到最终索引的函数

def ensure_dir(d: str): os.makedirs(d, exist_ok=True)

def _merge_runs_to_run(in_runs: List[str], out_run: str) -> Tuple[int, int]:
    """
    单进程：把若干 run (TSV) 归并成一个新的 run (TSV)。
    返回 (rows_written, in_runs_count) 方便日志统计。
    """
    import heapq
    # heap: (term, docid, run_idx, tf)
    readers = [RunReader(p) for p in in_runs]
    heap = []
    for i, r in enumerate(readers):
        try:
            t, d, tf = next(r)
            heap.append((t, d, i, tf))
        except StopIteration:
            pass
    heapq.heapify(heap)

    rows = 0
    with RunWriter(out_run) as w:
        cur_term = None
        cur_doc = -1
        cur_tf = 0

        def flush_term_doc():
            nonlocal cur_term, cur_doc, cur_tf, rows
            if cur_term is not None and cur_doc >= 0:
                # 写一行 term \t docid \t tf
                # 直接走 writer 的低层写接口（你也可先缓冲再一次性写）
                w._f.write(f"{cur_term}\t{cur_doc}\t{cur_tf}\n")
                rows += 1

        while heap:
            term, docid, i, tf = heapq.heappop(heap)

            if cur_term is None:
                cur_term, cur_doc, cur_tf = term, docid, tf
            elif term != cur_term:
                flush_term_doc()
                cur_term, cur_doc, cur_tf = term, docid, tf
            else:
                # same term, merge by docid
                if docid == cur_doc:
                    cur_tf += tf
                else:
                    flush_term_doc()
                    cur_doc, cur_tf = docid, tf

            # advance that run
            try:
                t2, d2, tf2 = next(readers[i])
                heapq.heappush(heap, (t2, d2, i, tf2))
            except StopIteration:
                pass

        flush_term_doc()

    for r in readers:
        try: r.close()
        except Exception: pass

    return rows, len(in_runs)

def parallel_merge_to_index(
    run_paths: List[str],
    fanin: int = 8,
    workers: int | None = None,
    tmpdir: str = "data/tmp_merge",
    postings_path: str = POSTINGS_PATH,
    lexicon_path: str = LEXICON_PATH,
):
    """
    多轮并行：run TSV -> (多轮归并) -> 单一 run TSV -> 最终 postings/lexicon
    """
    ensure_dir(tmpdir)
    round_id = 0
    current = list(sorted(run_paths))

    while len(current) > 1:
        groups = [current[i:i+fanin] for i in range(0, len(current), fanin)]
        next_round: List[str] = []

        with ProcessPoolExecutor(max_workers=workers) as ex:
            futs = {}
            for g in groups:
                out_run = os.path.join(tmpdir, f"r{round_id}_{uuid.uuid4().hex[:8]}.tsv")
                fut = ex.submit(_merge_runs_to_run, g, out_run)
                futs[fut] = out_run

            for fut in as_completed(futs):
                rows, cnt = fut.result()
                out_run = futs[fut]
                next_round.append(out_run)
                print(f"[pmerge] round={round_id} merged {cnt} runs -> {out_run} rows={rows}")

        # 可选：清理上一轮的输入 run（如果确认不再需要）
        # for p in current: os.remove(p)

        current = next_round
        round_id += 1

    # 只剩一个 run：写最终索引
    final_run = current[0]
    print(f"[pmerge] FINAL run: {final_run} -> {postings_path}, {lexicon_path}")
    merge_runs_to_index([final_run], postings_path, lexicon_path)  # 复用你现有的最终合并函数
    # 可选：清理 final_run
    # os.remove(final_run)

def main():
    ap = argparse.ArgumentParser(description="Parallel layered merge of TSV runs into final index.")
    ap.add_argument("runs", nargs="+", help="Input run_*.tsv paths")
    ap.add_argument("--fanin", type=int, default=8, help="Max runs merged per process per round")
    ap.add_argument("--workers", type=int, default=None, help="#processes (default: cpu_count())")
    ap.add_argument("--tmpdir", default="data/tmp_merge", help="Directory for intermediate runs")
    args = ap.parse_args()

    parallel_merge_to_index(
        run_paths=args.runs,
        fanin=args.fanin,
        workers=args.workers,
        tmpdir=args.tmpdir,
    )

if __name__ == "__main__":
    main()
