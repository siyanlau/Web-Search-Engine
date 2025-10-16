# Web Search Engine

## 1) How to run (end-to-end)

### Build binary runs (multiprocessing)

```bash
# tune workers ~= physical cores; larger batch_size → fewer runs (faster merge)
python -m engine.build_runs_mp --input data/collection.tsv \
  --outdir data/runs --batch-size 100000 --workers 8
```

This produces many **binary RUN1** files in `data/runs/` and writes a consistent `data/doc_lengths.pkl` aligned to the docIDs built in this step. (Doc-length persistence is done once after collecting worker outputs.) 

### Shrink runs with a parallel one-round merge (e.g., 89 → 12, fan-in 8)

```bash
python -m engine.parallel_merge data/runs/*.run \
  --fanin 8 --workers 8 --rounds 1 --tmpdir data/tmp_merge > last_round.txt
# On Windows, prefer globbing over piping a file with CRLF.
```

### Final merge to the on-disk index (blocked postings + lexicon)

```bash
# Use a glob to avoid CRLF issues:
python -m engine.merger data/tmp_merge/round_0000/*.run --codec raw --block 128
```

This writes `data/index.postings` (binary, compressed, and block-oriented) and `data/index.lexicon` (pickle directory).

### Search (Boolean DAAT / BM25)

```python
from engine.searcher import Searcher
s = Searcher()                                  # loads lexicon/postings/doc_lengths
print(s.search("machine learning", topk=10))     # BM25, OR by default
print(s.search("communication policy", mode="AND"))  # Boolean AND
```

---

## 2) System architecture

```
              ┌────────────────────────┐
 TSV Corpus → │ build_runs_mp          │  parses+tokenizes per batch
              │ - in-memory per-batch  │  → RUN1 files: data/runs/run_*.run
              │ - writes RUN1          │  → doc_lengths.pkl (global, once)
              └──────────┬─────────────┘
                         │ many runs (89 if you use default params)
                         ▼
              ┌────────────────────────┐
              │ parallel_merge         │  layered, per-group k-way merge
              │ - fan-in (e.g., 8)     │  → fewer RUN1s in data/tmp_merge/round_xxxx
              │ - N workers            │
              └──────────┬─────────────┘
                         │ few runs (e.g., 12)
                         ▼
              ┌────────────────────────┐
              │ final merger           │  global k-way merge
              │ - ListWriter blocks    │  → index.postings (binary)
              │ - write lexicon        │  → index.lexicon (pickle)
              └──────────┬─────────────┘
                         │
                         ▼
              ┌────────────────────────┐
              │ searcher (DAAT/BM25)   │
              └────────────────────────┘
```

**Binary RUN1** (intermediate): grouped by term; for each term store `n`, then `n` `uint32` docIDs and `n` `uint32` freqs (little-endian). The reader streams group-by-term with minimal copies.

**Final index**: `index.postings` is **block-oriented** (default 128 docs/block) with codecs (`raw`, `varbyte`). `index.lexicon` stores for each term: postings file offset, `df`, number of blocks, and a per-block directory (`offset`, `last_docid`, `doc_bytes`, `freq_bytes`, `codec`). This directory enables fast block seeks/streaming, because you know exactly where to find a block, given a term and a docID.

---

## 3) Components & how they connect

* **`parser.py`** — robust TSV text cleaning + tokenization (keeps tokens like `u.s.` or `3.14` intact).
* **`build_runs_mp.py`** — multiprocessing builder of **sorted RUN1** files; aggregates per-doc lengths from workers and writes `doc_lengths.pkl` once so it matches the docID universe of these runs. 
* **`runio.py`** — `BinaryRunWriter/Reader`: grouped-binary RUN1 I/O; the reader uses memoryviews for efficient iteration.
* **`parallel_merge.py`** — layered parallel merge; per job k-way merges up to `fanin` runs and writes a new RUN1; `--rounds` lets you stop after 1 round (e.g., 89→12).
* **`merger.py`** — final single-writer k-way merge: maintain a heap of (term, docid, tf, src); when the term changes, flush the accumulated `{docid: tf}` to the `ListWriter` (block encoder) and record the returned lexicon entry; at the end, save lexicon and close writer. 
* **`listio.py`** — `ListWriter/Reader`: block layout, `raw`/`varbyte` codecs, per-block directory, sequential and random access.
* **`lexicon.py`** — persistent map `term → {offset, df, nblocks, blocks[]}` (pickle).
* **`searcher.py`** — façade that loads the lexicon and postings reader; wires **Boolean DAAT** (`daat.py`) and **BM25** (`daat_ranker.py`), using `doc_lengths.pkl` for ranking.

---

## 4) Internals (why it’s fast enough)

* **Binary RUN1 removes text parsing overhead** during merge (no `split()/int()` hot-loops).
* **Layered parallel merge** reduces fan-in before the last pass. The final pass must keep global term order and single output pointer, so we put parallelism before it.
* **Blocked postings + per-block directory** let DAAT cursors skip/seek efficiently and decode in small chunks; `raw` keeps decoding trivial; `varbyte` trades CPU for I/O.

---

## 5) Measured build & search times (from `benchmark.txt`)

### Full dataset (~8.84M docs)

* **Build runs** (workers=8, batch=100k): `real 4m32.633s` (89 runs) 
* **Parallel merge** (one round, fanin=8, workers=8): `real 5m10.382s` → **12 outputs** 
* **Final merger** (12 → index, `--codec raw --block 128`): `real 18m33.593s` (postings 2,815,133,072 bytes; 3,321,136 terms)     
* **Search** DAAT search on the scale of 0.001s. Typically, `OR` takes longer than `AND`. 

### 1M-doc slice (for reproducibility)

* **Parser only**: `real 1m43.765s` 
* **Direct indexer (baseline)**: `real 2m17.967s` (686,254 terms) 
* **Build runs (batch=100k, workers=4)**: `real 1m7.835s` (10 runs) 
* **Build runs (batch=50k, workers=4)**: `real 1m3.639s` (20 runs) 
* **Merger** (these runs → index): `real 1m59.295s` 
* **Search** DAAT search on the scale of 0.001s. Typically, `OR` takes longer than `AND`. 

> Notes: The final pass is single-writer by design; the time reductions primarily come from (a) fewer runs via larger `batch_size`, and (b) one-round parallel merge to shrink 89→~12 before the final k-way.

---

## 6) Final index sizes

* `index.postings`: **~2,749,154 KB (~2.62 GiB)**
* `index.lexicon`: **~314,030 KB**
* `doc_lengths.pkl`: **~60,340 KB**

---

## 7) Limitations

* **Final merge is not parallel** (global order + single sink). This is unavoidable in my opinion, but Python made it a particularly bad bottleneck.  
* No positional index; postings hold docIDs + TFs only.
* No impact-ordered pruning (WAND/BMW) yet; DAAT is correctness-first.

---

## 8) Tuning cheat-sheet

* **`build_runs_mp`**: pick `batch_size` as large as RAM allows to reduce run count; set `--workers` ≈ physical cores.
* **`parallel_merge`**: start with `--fanin 8 --workers 8 --rounds 1` to halve end-to-end time for big K; if CPU and disk are under-utilized, try `workers=10`.
* **`merger`**: `--block 128` is a good default; `--codec raw` keeps queries fast. If I/O dominates and CPU is idle, try `--codec varbyte` to shrink postings (you already tested correctness/perf).

---

## 9) File map (where things land)

* `data/runs/run_*.run` — binary RUN1 intermediate files (grouped by term).
* `data/tmp_merge/round_0000/run_*.run` — outputs of the one-round parallel merge.
* `data/index.postings` — final **blocked** postings (binary).
* `data/index.lexicon` — lexicon with per-term block directory (pickle).
* `data/doc_lengths.pkl` — `docid → length` used by BM25.