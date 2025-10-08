
"""
verify_doclens_consistency.py

Purpose:
  Verify consistency between an inverted index and doc_lengths, and diagnose
  common mismatch scenarios (e.g., index and doc_lengths generated with
  different limits or at different times). Also compares against a fresh parse
  of MARCO_TSV to help identify which artifact is stale.

Run (from project root):
  python verify_doclens_consistency.py
  # or
  python verify_doclens_consistency.py --show-samples 15

Assumptions:
  - engine/paths.py defines INDEX_PATH, DOC_LENGTHS_PATH, MARCO_TSV_PATH
  - engine/utils.py provides load_index, load_doc_lengths
  - engine/parser.py provides Parser with parse_docs(path, limit=None)
"""

import argparse
import os
from collections import defaultdict

# Try absolute imports assuming script runs from project root
try:
    from engine.paths import INDEX_PATH, DOC_LENGTHS_PATH, MARCO_TSV_PATH
    from engine.utils import load_index, load_doc_lengths
    from engine.parser import Parser
except Exception as e:
    raise RuntimeError(
        "Failed to import engine.* modules. "
        "Run this script from the project root (e.g., `python verify_doclens_consistency.py`).\n"
        f"Original import error: {e}"
    )


def reconstruct_doc_lengths_from_index(index: dict[int, dict[int, int]]) -> dict[int, int]:
    """
    Reconstruct document lengths from an inverted index by summing term frequencies
    across all terms for each document: dl(d) = sum_t tf(t, d).

    Args:
        index: dict[str, dict[int, int]] mapping term -> {docid: tf}

    Returns:
        dict[int, int]: reconstructed docid -> length
    """
    dl = defaultdict(int)
    for postings in index.values():
        for docid, tf in postings.items():
            dl[docid] += tf
    return dict(dl)


def summarize_set_diff(a: set, b: set, label_a: str, label_b: str, show: int = 10):
    """
    Print a concise summary of set membership differences.
    """
    only_a = sorted(a - b)
    only_b = sorted(b - a)
    print(f"\n=== Membership Diff: {label_a} vs {label_b} ===")
    print(f"Total {label_a}: {len(a)}")
    print(f"Total {label_b}: {len(b)}")
    print(f"Only in {label_a}: {len(only_a)}")
    print(f"Only in {label_b}: {len(only_b)}")
    if only_a[:show]:
        print(f"  Sample only-in-{label_a} (up to {show}): {only_a[:show]}")
    if only_b[:show]:
        print(f"  Sample only-in-{label_b} (up to {show}): {only_b[:show]}")


def main(show_samples: int):
    print("=== Loading artifacts ===")
    print(f"INDEX_PATH       = {INDEX_PATH}")
    print(f"DOC_LENGTHS_PATH = {DOC_LENGTHS_PATH}")
    print(f"MARCO_TSV_PATH   = {MARCO_TSV_PATH}")

    if not os.path.exists(INDEX_PATH):
        raise FileNotFoundError(f"Index file not found at {INDEX_PATH}")
    if not os.path.exists(DOC_LENGTHS_PATH):
        raise FileNotFoundError(f"Doc lengths file not found at {DOC_LENGTHS_PATH}")
    if not os.path.exists(MARCO_TSV_PATH):
        raise FileNotFoundError(f"MARCO TSV not found at {MARCO_TSV_PATH}")

    index = load_index(INDEX_PATH)
    doc_lengths = load_doc_lengths(DOC_LENGTHS_PATH)

    # Sets of docids
    docs_in_index = set()
    for postings in index.values():
        docs_in_index.update(postings.keys())
    docs_in_doclens = set(doc_lengths.keys())

    print("\n=== Basic Stats ===")
    print(f"Terms in index:            {len(index):,}")
    print(f"Docs referenced by index:  {len(docs_in_index):,}")
    print(f"Docs in doc_lengths:       {len(docs_in_doclens):,}")

    # 1) Membership differences between index and doc_lengths
    summarize_set_diff(docs_in_index, docs_in_doclens, "index-docs", "doc_lengths", show_samples)

    # 2) Reconstruct doc_lengths from index and compare values on overlap
    print("\n=== Reconstructing doc lengths from index (Σ TF per doc) ===")
    reconstructed = reconstruct_doc_lengths_from_index(index)
    docs_in_reconstructed = set(reconstructed.keys())
    summarize_set_diff(docs_in_index, docs_in_reconstructed, "index-docs", "reconstructed-docs", show_samples)

    # Compare lengths where both sources have the docid
    overlap = sorted(docs_in_doclens & docs_in_reconstructed)
    diffs = []
    for d in overlap:
        if doc_lengths[d] != reconstructed[d]:
            diffs.append((d, doc_lengths[d], reconstructed[d]))
    print(f"\nDocs with length mismatch (doc_lengths vs reconstructed): {len(diffs)}")
    if diffs[:show_samples]:
        print("  Sample mismatches:")
        for d, a, b in diffs[:show_samples]:
            print(f"    doc {d}: stored={a}, reconstructed={b} (Δ={b-a})")

    # 3) Fresh parse as ground truth sanity check
    print("\n=== Fresh parse sanity check on TSV ===")
    parser = Parser()
    fresh_docs, fresh_dl = parser.parse_docs(MARCO_TSV_PATH)  # no limit; respects parser's filtering
    fresh_docids = set(fresh_docs.keys())

    summarize_set_diff(fresh_docids, docs_in_index, "fresh-parse-docs", "index-docs", show_samples)
    summarize_set_diff(fresh_docids, docs_in_doclens, "fresh-parse-docs", "doc_lengths", show_samples)

    # Compare values where both fresh and stored doc_lengths have the docid
    overlap_fresh = sorted(fresh_docids & docs_in_doclens)
    dl_mismatch = []
    for d in overlap_fresh:
        if fresh_dl[d] != doc_lengths[d]:
            dl_mismatch.append((d, fresh_dl[d], doc_lengths[d]))
    print(f"\nDocs with length mismatch (fresh parse vs stored doc_lengths): {len(dl_mismatch)}")
    if dl_mismatch[:show_samples]:
        print("  Sample mismatches:")
        for d, a, b in dl_mismatch[:show_samples]:
            print(f"    doc {d}: fresh={a}, stored={b} (Δ={b-a})")

    print("\n=== Conclusion Hints ===")
    print("- If 'Only in index-docs' is non-empty: index references docs missing from doc_lengths (likely different parse limits).")
    print("- If 'Only in doc_lengths' is non-empty: doc_lengths contains docs not present in index (again, likely limits/different input).")
    print("- If many value mismatches vs reconstructed: your doc_lengths file may have been computed with a different tokenizer/parse revision.")
    print("- If mismatches vs fresh parse: regenerate both artifacts together from the same parser version and input limits.")
    print("\nDone.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--show-samples", type=int, default=10, help="How many sample ids to print in diffs")
    args = ap.parse_args()
    main(show_samples=args.show_samples)
