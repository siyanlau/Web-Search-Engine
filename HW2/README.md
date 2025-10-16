# Web Search Engine — README (System Overview & Usage)

## TL;DR

This project builds a scalable **blocked inverted index** from a TSV corpus and supports **Boolean DAAT** as well as **BM25 top-K ranking**. The indexing pipeline is:
**TSV → (multiprocessing) sorted runs → (parallel layered) merge to a single run → final merger → `index.postings` + `index.lexicon` + `doc_lengths.pkl` → search**.
Final postings are stored in a compact **block-oriented binary format**; the lexicon stores per-term offsets, df, and an optional per-block directory for fast seeks. 

---

## What the program can do

* Parse MS MARCO–style TSV, clean text (HTML unescape + ftfy), and tokenize robustly. 
* Build **sorted runs** in parallel (one process per batch) and persist **document lengths** aligned with the run docIDs. 
* Merge runs:

  * **Parallel layered merge** (configurable fan-in/workers) to reduce N runs to a few large runs. 
  * **Final single-process merger** writes blocked postings and the lexicon. 
* Search:

  * **Boolean AND/OR** using DAAT cursors.
  * **BM25 Top-K** ranking with DAAT (no pruning yet; AND mode optionally requires all query terms).  

---

## How to run

### 0) Data & paths

Default paths live in `engine/paths.py` (postings, lexicon, doc lengths, corpus TSV). Adjust as needed. 

### 1) Build sorted runs (multiprocessing)

```bash
# Windows PowerShell / bash alike
python -m engine.build_runs_mp --input data/marco_medium.tsv --outdir data/runs --batch-size 200000 --workers <N>
```

This streams the TSV, tokenizes per line, builds an in-memory inverted index per batch, writes a **sorted run** for that batch, and saves `doc_lengths.pkl` once (using consistent docIDs).  

> Tip: choose `workers` ≈ number of physical cores; increase `batch-size` to reduce the number of runs (fewer merges later). 

### 2) (Optional) Parallel layered merge (reduce many runs → few)

```bash
# One-round example: fan-in 8, 16 workers
python -m engine.parallel_merge data/runs/*.tsv --fanin 8 --workers 16 --tmpdir data/tmp_merge
# Produced runs are printed per round; the last round is the input to final merger.
```

Each worker performs a k-way merge of (up to) `fanin` runs and emits a new **run (TSV)**. Rounds continue until only one run remains. 

### 3) Final merge → binary postings + lexicon

```bash
# replace <runs_or_last_round.txt> with your last-run list
python -m engine.merger data/tmp_merge/*.tsv
# or explicitly: python -m engine.merger run_a.tsv run_b.tsv ...
```

The merger does a k-way stream over all input runs, **aggregates tf for identical (term, docid)**, and writes **blocked postings** via `ListWriter`, while recording per-term metadata into the lexicon.  

### 4) Search

```python
from engine.searcher import Searcher
s = Searcher()                # loads lexicon/postings/doc_lengths from defaults
print(s.search("machine learning", topk=10))        # BM25, OR-mode by default
print(s.search("machine learning", mode="AND"))     # Boolean AND
```

The BM25 search path reuses the already opened lexicon and postings reader when present to avoid reopen cost. 

---

## Internal design

### File formats

**Lexicon (`index.lexicon`, pickle).** For each term:
`offset` of the first block, `df`, `nblocks`, and optionally a **per-block directory** with `(offset,last_docid,doc_bytes,freq_bytes)`. This enables **O(log B)** seeks and efficient block streaming. 

**Postings (`index.postings`, binary).** Written in **blocks** (default 128 docIDs). Within a block: **docID deltas** and **freqs** are encoded; the writer supports `raw` or `varbyte` codecs (switchable). The reader can stream blocks, random-seek to blocks (via the lexicon), and decode per block. *(See `ListWriter`/`ListReader` for codec & block machinery.)* 

**Intermediate “run” files.**

* TSV runs: `term \t docid \t tf`; used by the parallel layered merge. `RunWriter`/`RunReader` provide a stable interface.  
* Binary runs (RUN1): for faster single-process merges. Format: `MAGIC="RUN1"`, then per term: `[len_term, term_utf8, n, docid[n], freq[n]]`; the reader iterates **group-by-term** with memoryviews to avoid extra copies.  

### Indexing pipeline

1. **Tokenization/Parsing.** `Parser` cleans text (ftfy + HTML) and tokenizes (keeps tokens like *u.s.*, *3.14* as single tokens). 

2. **Per-batch inverted index.** `Indexer.build_inverted_index` accumulates a dict `term -> {docid: tf}`. 

3. **Run building (MP).** Each worker writes a sorted run and returns per-doc lengths; the main process persists `doc_lengths.pkl` once so docIDs are consistent across later stages.  

4. **Merging.**

   * **Parallel layered merge**: groups runs (`fanin`) and merges them concurrently into fewer runs per round. 
   * **Final merger**: k-way heap over `(term, docid, tf)` streams; on term boundary, write blocks via `ListWriter` and record the lexicon entry.  

5. **Searching.**

   * **Boolean DAAT** uses `PostingsCursor` over `ListReader` with ascending-df term ordering for AND to reduce cursor work. 
   * **Ranked DAAT (BM25)** maintains a min-heap of size *K*, scores tied docIDs across cursors, and supports `mode="OR"`/`"AND"`. 

---

## How long it takes & index sizes

Results vary by hardware and parameters (`batch-size`, `fanin`, codec). The repo includes a small benchmark log; for **1M docs**, building runs and indexing complete within a couple of minutes on a typical desktop, and DAAT queries are ~millisecond scale (Boolean set-equality tests and BM25 samples included there).  

On your 8C/16T Windows machine, we observed during final merge (TSV→binary) a streaming throughput in the **~10–12M postings/min** ballpark based on partial profiles (heap-based k-way merge dominated by `heappop/push` and file writes). These are representative numbers; rerun on your setup to report final wall times.

To reproduce and report:

```bash
# Sizes
python - <<'PY'
import os
for p in ["data/index.postings","data/index.lexicon","data/doc_lengths.pkl"]:
    if os.path.exists(p):
        print(p, os.path.getsize(p), "bytes")
PY

# Timing (Linux/macOS: `time`; Windows PowerShell: `Measure-Command`)
time python -m engine.build_runs_mp --input data/marco_medium.tsv --outdir data/runs --batch-size 200000 --workers 8
time python -m engine.parallel_merge data/runs/*.tsv --fanin 8 --workers 16 --tmpdir data/tmp_merge
time python -m engine.merger data/tmp_merge/*.tsv
```

---

## Limitations

* **Final merge is single-process** (I/O friendly but CPU under-utilized). A fully parallel final writer would need lock-free region allocation or per-term sharding.
* **No positional index**; postings store only docIDs and tfs.
* **No pruning** (WAND / BMW) in ranked DAAT yet; correctness-first. 
* **Simple tokenization** (ASCII-ish, keeps `u.s.` etc.); language-specific processing and stemming are out of scope. 
* **doc_lengths** must match the docID universe of the index; if you rebuild runs from a different slice, **rebuild `doc_lengths.pkl`** accordingly to avoid `KeyError` at search time (seen when switching between 1M vs full runs). 

---

## Module map (what the major modules do)

* `engine/parser.py` — robust TSV parser & tokenizer; also provides `iter_docs` for streaming. 
* `engine/indexer.py` — in-memory index (testing path) and save to blocked binary; replaced in production by runs+merge.  
* `engine/runio.py` — TSV `RunWriter/RunReader`; **binary RUN1** writer/reader for faster merges.  
* `engine/build_runs_mp.py` — multiprocessing run builder; writes `doc_lengths.pkl` once aligned with docIDs. 
* `engine/parallel_merge.py` — layered parallel merge (configurable `--fanin`, `--workers`), outputs fewer larger runs. 
* `engine/merger.py` — final k-way merge from runs to **blocked postings** + **lexicon**. 
* `engine/listio.py` — low-level blocked postings I/O, codecs (e.g., varbyte/raw), block directory support. 
* `engine/lexicon.py` — term → metadata (offset/df/nblocks/blocks), pickle persistence. 
* `engine/daat.py` — Boolean DAAT primitives (`PostingsCursor`, AND/OR).
* `engine/daat_ranker.py` — **BM25 DAAT Top-K** (no pruning), used by `Searcher.search(...)`. 
* `engine/searcher.py` — convenience façade; reuses cached lexicon/reader; supports Boolean vs BM25 paths. 
* `engine/paths.py` — centralizes file paths and constants. 
* `engine/utils.py` — misc utilities (e.g., doc length persistence).

---

## How it works (a bit deeper)

* **DAAT iteration.** We maintain one cursor per query term, always advancing the cursor(s) pointing to the current smallest docID; AND-mode requires all terms to match before scoring. Tied cursors are advanced together. 
* **BM25 math.** Standard BM25 with tunables `(k1,b)`; we pre-compute IDF from `(N, df)` and use `avgdl` from `doc_lengths`.  
* **Blocks & seeking.** The lexicon’s optional per-block directory allows binary search over blocks by `last_docid` and precise `offset` jumps, minimizing decode work on large lists. 

---

## Reproducibility & environment

* **Windows notes.** Use `python -m ...` (module mode) and ensure argument lists don’t carry CRLF in “@file” expansions (the error `OSError: ... '\r'` came from a trailing CR).
* **Multiprocessing.** On Windows the `if __name__ == "__main__"` guard is already in place in `build_runs_mp.py`. 

---

## Troubleshooting

* **`KeyError: <docid>` during BM25** — your `doc_lengths.pkl` doesn’t match the docID space of the index (e.g., you briefly indexed a different slice). Rebuild runs and doc lengths together. 
* **Slow final merge** — expected to be CPU-light single-writer. Use **parallel layered merge** to reduce inputs first; tune `--fanin` and `--workers`. 
* **TSV vs binary runs** — the final merger accepts either; **binary RUN1** reduces Python parsing overhead when merging many runs. 

---

## Future work

* Block-max scores and WAND/BMW pruning;
* Positional index and phrase queries;
* Better compression (PForDelta/SIMD-BP128) at block level;
* Fully parallel final writer (term-range sharding to avoid contention).

---

### Appendix: Command cookbook

```bash
# 1) Build runs (and doc lengths)
python -m engine.build_runs_mp --input data/marco_medium.tsv --outdir data/runs --batch-size 200000 --workers 8

# 2) Parallel layered merge (reduce many→few)
python -m engine.parallel_merge data/runs/*.tsv --fanin 8 --workers 16 --tmpdir data/tmp_merge

# 3) Final merge
python -m engine.merger data/tmp_merge/*.tsv

# 4) Search examples
python - <<'PY'
from engine.searcher import Searcher
s = Searcher()
print("BM25:", s.search("neural networks", topk=10))
print("Boolean AND:", s.search("neural networks", mode="AND"))
PY
```