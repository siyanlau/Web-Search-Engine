
"""
test_three_runs_merge.py

Integration test for the "three 10k runs → k-way merge" workflow.
"""

import argparse
import os
import random
from collections import defaultdict

from engine.paths import MARCO_TSV_PATH, DOC_LENGTHS_PATH, POSTINGS_PATH, LEXICON_PATH
from engine.utils import write_doc_lengths
from engine.parser import Parser
from engine.indexer import Indexer
from engine.runio import RunWriter
from engine.merger import Merger
from engine.lexicon import Lexicon
from engine.listio import ListReader, ListWriter
from engine.searcher import Searcher

TMP_DIR = "data/tmp_chunks"
RUNS_DIR = "data/runs"
DIRECT_POSTINGS = "data/index_direct.postings"
DIRECT_LEXICON = "data/index_direct.lexicon"

def ensure_dirs():
    os.makedirs(TMP_DIR, exist_ok=True)
    os.makedirs(RUNS_DIR, exist_ok=True)

def split_into_chunks(input_path, chunk_size=10000):
    paths = []
    with open(input_path, "r", encoding="utf-8") as fin:
        for i in range(3):
            out = os.path.join(TMP_DIR, f"chunk{i+1}.tsv")
            with open(out, "w", encoding="utf-8") as fout:
                n = 0
                while n < chunk_size:
                    line = fin.readline()
                    if not line:
                        break
                    if not line.strip():
                        continue
                    fout.write(line)
                    n += 1
            paths.append(out)
    return paths

def shift_docids(docs, base):
    if base == 0:
        return docs
    return {docid + base: toks for docid, toks in docs.items()}

def build_run_from_chunk(chunk_path, base_offset, run_path):
    parser = Parser()
    docs, lens = parser.parse_docs(chunk_path)
    # docs = shift_docids(docs, base_offset)
    # lens = {docid + base_offset: l for docid, l in lens.items()}

    indexer = Indexer()
    postings = indexer.build_inverted_index(docs)

    rw = RunWriter(run_path)
    rw.write_from_index(postings)
    rw.close()

    return lens

def write_direct_index_all(docs, postings_path, lexicon_path, block_size=128):
    indexer = Indexer()
    index = indexer.build_inverted_index(docs)
    writer = ListWriter(postings_path, block_size=block_size)
    lex = Lexicon()
    for term, plist in index.items():
        entry = writer.add_term(term, plist)
        lex.add(term, entry)
    writer.close()
    lex.save(lexicon_path)

def compare_postings(lex_path_a, post_path_a, lex_path_b, post_path_b, samples=200, seed=7):
    lex_a = Lexicon.load(lex_path_a).map
    lex_b = Lexicon.load(lex_path_b).map
    reader_a = ListReader(post_path_a)
    reader_b = ListReader(post_path_b)

    terms = list(set(lex_a.keys()) & set(lex_b.keys()))
    if not terms:
        print("[Compare] No overlapping terms between lexicons.")
        return

    random.seed(seed)
    sample_terms = random.sample(terms, min(samples, len(terms)))
    fail = 0
    for t in sample_terms:
        ea, eb = lex_a.get(t), lex_b.get(t)
        da, fa = reader_a.read_postings(ea)
        db, fb = reader_b.read_postings(eb)
        if da != db or fa != fb:
            fail += 1
            print(f"[DIFF] term='{t}'  A(df)={len(da)}  B(df)={len(db)}")
    reader_a.close()
    reader_b.close()
    ok = len(sample_terms) - fail
    print(f"[Compare] Terms checked={len(sample_terms)}  OK={ok}  DIFF={fail}")

def main(args):
    ensure_dirs()

    chunk_paths = split_into_chunks(MARCO_TSV_PATH, chunk_size=args.chunk)
    print(f"[Split] Chunks: {chunk_paths}")

    total_lens = {}
    run_paths = []
    base_offsets = [0, args.chunk, 2*args.chunk]
    for i, (chunk, base) in enumerate(zip(chunk_paths, base_offsets), start=1):
        run_path = os.path.join(RUNS_DIR, f"run{i}.tsv")
        print(f"[Run] Building run{i} from {chunk} with base={base}")
        lens = build_run_from_chunk(chunk, base, run_path)
        total_lens.update(lens)
        run_paths.append(run_path)

    print(f"[Merge] Merging runs: {run_paths}")
    merger = Merger(POSTINGS_PATH, LEXICON_PATH, block_size=128)
    merger.merge(run_paths)

    print(f"[DocLens] Writing unified doc lengths to {DOC_LENGTHS_PATH}  (N={len(total_lens)})")
    write_doc_lengths(total_lens, DOC_LENGTHS_PATH)

    print("[Direct] Building direct in-memory index for full 30k...")
    parser = Parser()
    docs_full, lens_full = parser.parse_docs(MARCO_TSV_PATH, limit=3*args.chunk)
    write_direct_index_all(docs_full, DIRECT_POSTINGS, DIRECT_LEXICON, block_size=128)
    write_doc_lengths(total_lens, DOC_LENGTHS_PATH)
    
    print("[Compare] Merged vs Direct postings...")
    compare_postings(LEXICON_PATH, POSTINGS_PATH, DIRECT_LEXICON, DIRECT_POSTINGS, samples=args.samples)

    print("[Search] Sample queries on merged index (BM25 top 5):")
    s = Searcher(lexicon_path=LEXICON_PATH, postings_path=POSTINGS_PATH, doc_lengths=DOC_LENGTHS_PATH)
    sample_qs = ["communication policy", "machine learning", "u.s policy", "3.14 math"][:args.queries]
    for q in sample_qs:
        res = s.search(q, topk=5)
        print(f"  Q: {q}")
        for d, score in res:
            print(f"    {d}\t{score:.3f}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunk", type=int, default=10000)
    ap.add_argument("--samples", type=int, default=200)
    ap.add_argument("--queries", type=int, default=3)
    args = ap.parse_args()
    main(args)
