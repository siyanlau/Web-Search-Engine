# engine/searcher.py
from engine.lexicon import Lexicon
from engine.listio import ListReader
from engine.ranker import Ranker
from engine.utils import load_doc_lengths
from engine.paths import LEXICON_PATH, POSTINGS_PATH, DOC_LENGTHS_PATH

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
                # 若任一term无posting，交集为空，直接返回空
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


if __name__ == "__main__":
    # Run from project root:  python -m engine.searcher
    s = Searcher()  # uses default LEXICON_PATH / POSTINGS_PATH / DOC_LENGTHS_PATH if available

    print("=== Boolean mode sample ===")
    # Force boolean by constructing a Searcher without doc_lengths:
    s_bool = Searcher(doc_lengths=None)
    print("AND:", list(s_bool.search("overturned carriage", mode="AND"))[:5])
    print("OR :", list(sorted(s_bool.search("overturned carriage", mode="OR")))[:5])

    print("\n=== BM25 sample (top 10) ===")
    res = s.search("overturned carriage", topk=10)
    for docid, score in res:
        print(docid, round(score, 3))
