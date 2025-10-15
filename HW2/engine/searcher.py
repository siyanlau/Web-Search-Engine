# engine/searcher.py
from engine.lexicon import Lexicon
from engine.listio import ListReader
from engine.ranker import Ranker
from engine.utils import load_doc_lengths
from engine.paths import LEXICON_PATH, POSTINGS_PATH, DOC_LENGTHS_PATH
from engine.daat import PostingsCursor, boolean_and_daat, boolean_or_daat

class Searcher:
    """
    Blocked-index searcher.

    - Loads a small lexicon (term -> {offset, df, nblocks}) into memory.
    - Uses ListReader to read a term's postings on demand from the binary postings file.
    - If doc_lengths are provided, returns BM25-ranked results; otherwise falls back to AND/OR boolean results.
    """

    def __init__(self, lexicon_path: str = LEXICON_PATH, postings_path: str = POSTINGS_PATH, doc_lengths=None):
        # Load lexicon metadata (tiny, pickle-backed)
        self.lexicon = Lexicon.load(lexicon_path).map
        # Open postings binary file for on-demand reading
        self.reader = ListReader(postings_path)

        # doc_lengths can be:
        # - dict (preferred)
        # - str path to a pickle file
        # - None (boolean mode only)
        if isinstance(doc_lengths, dict):
            self.doc_lengths = doc_lengths
        elif isinstance(doc_lengths, str):
            self.doc_lengths = load_doc_lengths(doc_lengths)
        elif doc_lengths is None:
            # Try default path; if missing, boolean mode will be used
            try:
                self.doc_lengths = load_doc_lengths(DOC_LENGTHS_PATH)
            except Exception:
                self.doc_lengths = None
        else:
            raise TypeError(f"doc_lengths must be dict | str | None, got {type(doc_lengths)}")

    def _get_postings_dict(self, term: str):
        """
        Read a term's postings from disk and return as {docid: tf}.
        Returns empty dict if term not found.
        """
        entry = self.lexicon.get(term)
        if not entry:
            return {}
        docids, freqs = self.reader.read_postings(entry)
        return {d: f for d, f in zip(docids, freqs)}

    def search(self, query: str, mode="AND", topk=None):
        """
        Execute a query.
        - Ranked mode (BM25): returns list[(docid, score)] sorted by score desc.
        - Boolean mode: returns set[docid] (AND/OR).
        """
        q_terms = query.lower().split()

        # Ranked path (BM25)
        if self.doc_lengths:
            tiny_index = {}
            doc_sets = []
            for t in q_terms:
                pdict = self._get_postings_dict(t)  # {docid: tf}
                if pdict:
                    tiny_index[t] = pdict
                    doc_sets.append(set(pdict.keys()))

            if not tiny_index:
                return []

            # decide allowed docs by mode
            if mode == "AND":
                if not doc_sets:
                    return []
                allowed = set.intersection(*doc_sets)
                if not allowed:
                    return []
            elif mode == "OR":
                allowed = set.union(*doc_sets) if doc_sets else set()
            else:
                raise ValueError("mode must be AND or OR")

            ranker = Ranker(tiny_index, self.doc_lengths)
            scores = ranker.score(query)  # list[(docid, score)]

            # filter by allowed set to enforce AND/OR semantics in ranked mode
            scores = [(d, s) for (d, s) in scores if d in allowed]
            return scores[:topk] if topk else scores

        # Boolean fallback
        postings_sets = []
        for t in q_terms:
            entry = self.lexicon.get(t)
            if not entry:
                continue
            docids, _ = self.reader.read_postings(entry)
            postings_sets.append(set(docids))

        if not postings_sets:
            return set()

        if mode == "AND":
            return set.intersection(*postings_sets)
        elif mode == "OR":
            return set.union(*postings_sets)
        else:
            raise ValueError("mode must be AND or OR")

    def search_boolean_daat(self, query: str, mode: str = "AND"):
        terms = [t for t in query.lower().split() if t in self.lexicon]  # self.lex is Lexicon.map
        if not terms:
            return set()
        cursors = []
        for t in terms:
            entry = self.lexicon[t]
            cursors.append(PostingsCursor(self.reader, t, entry))
        if mode.upper() == "AND":
            return set(boolean_and_daat(cursors))
        elif mode.upper() == "OR":
            return set(boolean_or_daat(cursors))
        else:
            raise ValueError("mode must be AND or OR")

    # engine/searcher.py  (inside class Searcher)

    def search_topk_daat(self, query: str, topk: int = 10, k1: float = 1.2, b: float = 0.75, mode: str = "AND"):
        """
        DAAT + BM25 ranking (no pruning). Requires doc_lengths.

        This is a convenience wrapper that reuses the already-opened
        lexicon and postings reader (if present) to avoid reopen cost.
        It does not modify any of the existing search paths.
        """
        # We need document lengths for BM25. If absent, return no results.
        if not getattr(self, "doc_lengths", None):
            return []

        # Prefer the instance's cached lexicon map (e.g., self.lexicon),
        # otherwise load it once for this call.
        lex = getattr(self, "lexicon", None)
        if lex is None:
            from engine.lexicon import Lexicon
            from engine.paths import LEXICON_PATH
            lex = Lexicon.load(LEXICON_PATH).map

        # Prefer the instance's ListReader (e.g., self.reader). If the Searcher
        # does not hold one, create a temporary reader and close it on exit.
        reader = getattr(self, "reader", None)
        owns_reader = False
        if reader is None:
            from engine.listio import ListReader
            from engine.paths import POSTINGS_PATH
            reader = ListReader(POSTINGS_PATH)
            owns_reader = True

        try:
            from engine.daat_ranker import ranked_daat
            return ranked_daat(
                query=query,
                lex_map=lex,
                reader=reader,
                doc_lengths=self.doc_lengths,
                topk=topk,
                k1=k1,
                b=b,
                mode=mode,   # "AND" to match your baseline BM25, "OR" for disjunctive scoring
            )
        finally:
            if owns_reader:
                reader.close()



# if __name__ == "__main__":
#     # Run from project root:  python -m engine.searcher
#     s = Searcher()  # uses default LEXICON_PATH / POSTINGS_PATH / DOC_LENGTHS_PATH if available

#     print("=== Boolean mode sample ===")
#     # Force boolean by constructing a Searcher without doc_lengths:
#     s_bool = Searcher(doc_lengths=None)
#     print("AND:", list(s_bool.search("overturned carriage", mode="AND"))[:5])
#     print("OR :", list(sorted(s_bool.search("overturned carriage", mode="OR")))[:5])

#     print("\n=== BM25 sample (top 10) ===")
#     res = s.search("overturned carriage", topk=10)
#     for docid, score in res:
#         print(docid, round(score, 3))


if __name__ == "__main__":
    # Run from project root:  python -m engine.searcher
    import time
    from engine.paths import LEXICON_PATH, POSTINGS_PATH, DOC_LENGTHS_PATH
    from engine.lexicon import Lexicon
    from engine.listio import ListReader
    from engine.daat import PostingsCursor, boolean_and_daat, boolean_or_daat

    # Helper: normalize Searcher outputs to a set of docids
    def to_docid_set(obj):
        if isinstance(obj, set):
            return obj
        if isinstance(obj, list):
            if not obj:
                return set()
            first = obj[0]
            if isinstance(first, tuple) and len(first) == 2:  # [(docid, score)]
                return {d for d, _ in obj}
            if isinstance(first, int):  # [docid, ...]
                return set(obj)
        return set()

    # # Helper: DAAT boolean result (set of docids)
    # def daat_set(query: str, mode: str = "AND"):
    #     lex = Lexicon.load(LEXICON_PATH).map
    #     reader = ListReader(POSTINGS_PATH)
    #     terms = [t for t in query.lower().split() if t in lex]
    #     if not terms:
    #         reader.close()
    #         return set()
    #     cursors = [PostingsCursor(reader, t, lex[t]) for t in terms]
    #     if mode.upper() == "AND":
    #         res = set(boolean_and_daat(cursors))
    #     elif mode.upper() == "OR":
    #         res = set(boolean_or_daat(cursors))
    #     else:
    #         reader.close()
    #         raise ValueError("mode must be AND or OR")
    #     reader.close()
    #     return res
    
    lex = Lexicon.load(LEXICON_PATH).map
    reader = ListReader(POSTINGS_PATH)
    
    def daat_set(query: str, mode: str = "AND", lex_map=None, rd=None):
        from engine.daat import PostingsCursor, boolean_and_daat, boolean_or_daat
        lex_map = lex_map or lex
        rd = rd or reader
        terms = [t for t in query.lower().split() if t in lex_map]
        if not terms:
            return set()

        # AND：按 df 升序排列，减少推进成本（不改任何存储结构）
        if mode.upper() == "AND":
            terms.sort(key=lambda t: lex_map[t]["df"])

        cursors = [PostingsCursor(rd, t, lex_map[t]) for t in terms]
        if mode.upper() == "AND":
            return set(boolean_and_daat(cursors))
        else:
            # 小 k（≤2）时直接两路归并，避免 heap 常数
            if len(cursors) == 2:
                a, b = cursors
                out = set()
                da, db = a.docid(), b.docid()
                while da is not None and db is not None:
                    if da == db:
                        out.add(da); da = a.advance(); db = b.advance()
                    elif da < db:
                        out.add(da); da = a.advance()
                    else:
                        out.add(db); db = b.advance()
                while da is not None:
                    out.add(da); da = a.advance()
                while db is not None:
                    out.add(db); db = b.advance()
                return out
            #  min-heap
            from engine.daat import boolean_or_daat
            return set(boolean_or_daat(cursors))

    # Construct searchers
    s_full = Searcher()               # BM25 enabled (loads doc_lengths)
    s_bool = Searcher(doc_lengths=None)  # force boolean path

    queries = [
        "overturned carriage",
        "communication policy",
        "machine learning",
        "u.s policy",
        "3.14 math",
    ]

    print("=== Boolean vs DAAT (set equality & timing) ===")
    for q in queries:
        # Baseline boolean
        t0 = time.perf_counter()
        base_and = to_docid_set(s_bool.search(q, mode="AND"))
        t1 = time.perf_counter()
        base_or  = to_docid_set(s_bool.search(q, mode="OR"))
        t2 = time.perf_counter()

        # DAAT boolean
        t3 = time.perf_counter()
        daat_and = daat_set(q, mode="AND")
        t4 = time.perf_counter()
        daat_or  = daat_set(q, mode="OR")
        t5 = time.perf_counter()

        ok_and = (base_and == daat_and)
        ok_or  = (base_or == daat_or)

        print(f"\nQ: {q}")
        print(f"  AND equal: {ok_and} | sizes: base={len(base_and)} daat={len(daat_and)} "
              f"| time base={t1-t0:.4f}s daat={t4-t3:.4f}s")
        print(f"  OR  equal: {ok_or}  | sizes: base={len(base_or)} daat={len(daat_or)}   "
              f"| time base={t2-t1:.4f}s daat={t5-t3 - (t4-t3):.4f}s")

    print("\n=== BM25 sample (top 10) ===")
    res = s_full.search("manhattan project", topk=10)
    for docid, score in res:
        print(docid, round(score, 3))
