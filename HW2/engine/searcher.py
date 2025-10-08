# engine/searcher.py
from engine.ranker import Ranker
from engine.utils import load_index, load_doc_lengths
from engine.paths import INDEX_PATH, DOC_LENGTHS_PATH

class Searcher:
    """
    Simple in-memory searcher.
    Supports AND/OR search on an in-memory inverted index.

    Notes:
    - index is loaded from disk via engine.utils.load_index
    - If doc_lengths are provided (dict), BM25 ranking is enabled via Ranker.
      Otherwise, a boolean fallback (AND/OR) is used.
    """

    def __init__(self, index_path, format="pickle", doc_lengths=None):
        # `format` kept for API compatibility; currently unused since utils abstracts I/O.
        self.index = load_index(index_path)  # term -> {docid: freq}
        self.ranker = None

        # Accept either a dict (preferred), a path (str), or None.
        if isinstance(doc_lengths, dict):
            self.ranker = Ranker(self.index, doc_lengths)
        elif isinstance(doc_lengths, str):
            dl = load_doc_lengths(doc_lengths)
            self.ranker = Ranker(self.index, dl)
        elif doc_lengths is None:
            # No BM25; will use boolean mode.
            pass
        else:
            # Defensive: make misuse obvious (e.g., passing an int).
            raise TypeError(
                "doc_lengths must be a dict, a path string, or None. "
                f"Got {type(doc_lengths)}"
            )

    def search(self, query, mode="AND", topk=None):
        """
        Perform AND/OR search or BM25 ranking depending on availability of doc_lengths.
        Args:
            query: string with space-separated terms
            mode: "AND" or "OR" (used only for boolean mode)
            topk: optional cap for ranked results
        Returns:
            - Ranked mode: list[(docid, score)]
            - Boolean mode: set[docid]
        """
        if self.ranker:
            # BM25 scoring path
            scores = self.ranker.score(query)
            return scores[:topk] if topk else scores

        # Boolean fallback
        q_terms = query.lower().split()
        postings = [set(self.index[t].keys()) for t in q_terms if t in self.index]
        if not postings:
            return set()
        if mode == "AND":
            return set.intersection(*postings)
        elif mode == "OR":
            return set.union(*postings)
        else:
            raise ValueError("mode must be AND or OR")


if __name__ == "__main__":
    # Run from project root:  python -m engine.searcher
    # Expect index/doc_lengths already pickled by your pipeline.
    searcher = Searcher(INDEX_PATH, doc_lengths=DOC_LENGTHS_PATH)  # enable BM25
    result = searcher.search("communication", topk=10)
    for docid, score in result:
        print(docid, round(score, 3))
