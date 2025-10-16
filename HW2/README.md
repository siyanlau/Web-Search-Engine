# Web Search Engine Documentation

## 1. Overview

This system builds a **blocked inverted index** from a TSV corpus using a **binary run→merge** pipeline and supports **Boolean DAAT** and **BM25 top-K** search. Intermediate runs are stored in a compact **binary RUN1** format; the final index uses a compressed binary postings file plus a pickle lexicon that records per-term block directories for fast seeks.  

**Key components**

* **Binary runs (RUN1)** via `BinaryRunWriter/Reader`: grouped by term; for each term store `n`, then `n` docids and `n` freqs as little-endian uint32. Writer requires sorted `(term, docid)` order; reader streams group-by-term with minimal allocations. 
* **Final blocked postings** via `ListWriter/Reader`: per term the writer emits fixed-size blocks and returns a lexicon entry with `offset, df, nblocks, blocks[]` (each block has `offset, last_docid, doc_bytes, freq_bytes`, and `codec`). Reader can reconstruct full postings from the per-block directory, honoring `codec` (“raw” or “varbyte”).  
* **Lexicon**: a persistent `term → {offset, df, nblocks, blocks[]}` map saved as a pickle. Enables `O(log B)` random access over blocks and efficient sequential streaming.  

Paths are centralized in `engine/paths.py` (postings, lexicon, doc lengths, corpus path). 

---

## 2. Quickstart (CLI)

### 2.1 Build binary runs in parallel

```bash
python -m engine.build_runs_mp --input data/marco_medium.tsv \
  --outdir data/runs --batch-size 200000 --workers <PHYSICAL_CORES>
```

* Each worker tokenizes its batch, builds a small in-memory inverted index, and writes a **sorted binary run** (`*.run`).
* The main process aggregates and persists `doc_lengths.pkl` **once** so it matches the docID universe of these runs.  

### 2.2 Reduce runs with parallel layered merge (optional but recommended)

```bash
# Example: shrink many runs into ~ceil(K/fanin) bigger RUN1 files in one round
python -m engine.parallel_merge data/runs/*.run --fanin 8 --workers 8 --rounds 1 --tmpdir data/tmp_merge > last_round.txt
```

* Each job k-way merges up to `fanin` runs and **writes a new RUN1** (not the final index).
* `--rounds` lets you run exactly one round (e.g., 89 → 12). The output list is printed for the next step. 

### 2.3 Final merge → postings + lexicon

```bash
# Windows: prefer globbing to avoid CRLF issues
python -m engine.merger data/tmp_merge/round_0000/*.run
```

* Performs a global k-way merge over RUN1 streams; for each term, aggregates tf on identical `(term, docid)` and flushes a block-encoded postings list via `ListWriter`. Saves lexicon at the end.  

### 2.4 Search

```python
from engine.searcher import Searcher
s = Searcher()                                  # loads lexicon/postings/doc_lengths
print(s.search("machine learning", topk=10))     # BM25 (ranked), OR semantics by default
print(s.search("machine learning", mode="AND"))  # Boolean AND or ranked-AND
```

The `Searcher` loads the lexicon (pickle) and opens the binary postings reader; if `doc_lengths.pkl` is present it uses BM25 ranking, otherwise falls back to Boolean mode.  

---

## 3. Internal Pipeline

1. **Parallel run building** (`build_runs_mp`)

   * Stream TSV lines, batch them (`batch_size`), and assign contiguous docIDs starting from `start_docid`.
   * Per batch: tokenize → build inverted index → **BinaryRunWriter** emits a sorted RUN1 file.
   * Aggregate and save `doc_lengths.pkl` once (docid → length).   

2. **Layered parallel merge** (`parallel_merge`)

   * Group up to `fanin` runs; each worker k-way merges its group, summing tfs for identical `(term, docid)`, and writes a new **RUN1**.
   * Repeat for multiple rounds or stop early via `--rounds`. 

3. **Final merger** (`merger`)

   * Single-writer k-way merge over remaining RUN1s: maintain a heap of the current head from each run; whenever the term changes, flush the accumulated postings through `ListWriter.add_term()` and record the returned lexicon entry. 

---

## 4. File Formats

### 4.1 Binary RUN1 (intermediate)

* **Layout:** `MAGIC="RUN1"`; then for each term: `[len(term)][term_utf8][n][docid[0..n-1]][freq[0..n-1]]` as little-endian uint32 arrays.
* **Iteration:** `BinaryRunReader` streams `(term, docid, freq)` group-by-term without unnecessary copies. 

### 4.2 Blocked postings (final)

* Each term becomes `nblocks` blocks; each block stores encoded docIDs and freqs. Metadata per term (`offset, df, nblocks, blocks[]`) is returned by `ListWriter.add_term` and stored in the lexicon; the reader relies on this directory to read back full postings. Codecs: `"raw"` (4-byte uint32) or `"varbyte"` (docIDs as gaps + VB freqs).  

### 4.3 Lexicon

* A pickle mapping `term → entry`, where each entry includes `offset, df, nblocks, blocks[]`, enabling `O(log B)` seek and efficient streaming.  

---

## 5. Search Algorithms

* **Boolean DAAT**: `PostingsCursor` objects advance in docID order; AND sorts terms by increasing df to reduce work; OR uses either a specialized two-way merge or a heap-based n-way union. 
* **BM25 ranking**: `Ranker` computes standard BM25 with `(k1, b)` tunables; `Searcher.search()` builds a tiny per-query index from on-disk postings, filters by AND/OR semantics, then scores. Requires `doc_lengths.pkl`.   

---

## 6. Performance & Sizes (how to report)

* The codebase includes timing scaffolding in the CLI and `searcher.__main__` to print Boolean vs DAAT timing and BM25 samples. On typical desktop hardware, DAAT queries are low-millisecond scale on the 1M-doc slice; your full corpus will vary by SSD bandwidth and parameter choices (`batch_size`, `fanin`, codec).
* To report **index sizes**:

```bash
python - <<'PY'
import os
for p in ["data/index.postings","data/index.lexicon","data/doc_lengths.pkl"]:
    if os.path.exists(p): print(p, os.path.getsize(p), "bytes")
PY
```

* To test codec impact: build the same index twice by switching `ListWriter(codec="raw"|"varbyte")` and compare sizes vs query latencies using the same search harness. (The reader auto-detects the codec per term if left as `"auto"`.)  

---

## 7. Limitations

* **Final merge is single-writer** (global order must be preserved); the project uses layered parallel merge to reduce inputs aggressively and then a single pass to produce the final postings/lexicon. 
* **No positional index** (postings hold docIDs and term frequencies only).
* **No WAND/BMW pruning yet** in ranked DAAT; correctness first.
* **`doc_lengths.pkl` must match** the docID universe of the index; if you rebuild runs from a different slice, regenerate `doc_lengths.pkl` via the same run set to avoid KeyErrors in BM25. 

---

## 8. Module Guide

* `engine/parser.py` — robust tokenizer for TSV text; keeps tokens like `u.s.` or `3.14` intact; provides streaming APIs.  
* `engine/build_runs_mp.py` — multiprocessing builder for **binary RUN1** runs; persists `doc_lengths.pkl` after aggregating worker outputs.  
* `engine/runio.py` — `BinaryRunWriter/Reader` for grouped-binary runs (and legacy TSV helpers kept for tests). 
* `engine/parallel_merge.py` — layered parallel merge; inputs may be RUN1 or TSV; outputs **RUN1**; supports `--rounds`. 
* `engine/merger.py` — final k-way merge from runs to **blocked postings** + **lexicon**. 
* `engine/listio.py` — `ListWriter`/`ListReader` (blocked layout, raw/varbyte codecs, per-block directory).  
* `engine/lexicon.py` — pickle lexicon map: term → entry metadata (offset, df, nblocks, blocks). 
* `engine/searcher.py` — high-level façade that reuses already opened lexicon and reader; Boolean & ranked paths. 
* `engine/ranker.py` — BM25 implementation and end-to-end `score()` pipeline. 
* `engine/utils.py` — persistence helpers for `doc_lengths.pkl` and (legacy) pickled inverted indices. 
* `engine/paths.py` — central paths for `index.postings`, `index.lexicon`, `doc_lengths.pkl`, corpus TSV. 

---

## 9. Reproducibility & Environment Notes

* **Windows multiprocessing**: `if __name__ == "__main__": main()` guard is present; prefer `python -m engine.<module>` to avoid path surprises. 
* **CRLF paths**: when piping a list of files, ensure no trailing `\r` in argument files; or just use globbing (e.g., `round_0000/*.run`) as shown above.

---

## 10. Troubleshooting

* **`KeyError: <docid>` in BM25** → your `doc_lengths.pkl` doesn’t match the index’s docIDs (e.g., you briefly re-indexed a different slice). Rebuild runs and `doc_lengths.pkl` together from the same corpus slice. 
* **Searcher returns empty results for a term** → the term isn’t in the lexicon; `Searcher._get_postings_dict()` returns `{}` if not found. 
* **Slow final merge** → expected (single-writer). Use `parallel_merge` with a larger `batch_size` during build or smaller `fanin` per round to reduce input fan-in before the last pass. 