# engine/ranker.py
import math
from collections import defaultdict
from engine.utils import load_index, load_doc_lengths
from engine.paths import INDEX_PATH, DOC_LENGTHS_PATH

class Ranker:
    """
    BM25 ranker: computes document scores given an inverted index and doc lengths.

    Requirements / assumptions:
    - `index` is a mapping: term -> {docid: term_frequency}
    - `doc_lengths` is a mapping: docid -> document length (token count)
    - BM25 parameters k1 and b are configurable; defaults are common choices.
    """

    def __init__(self, index, doc_lengths, k1=1.2, b=0.75):
        self.index = index
        self.doc_lengths = doc_lengths
        self.k1 = k1
        self.b = b

        # Total number of documents
        self.N = len(doc_lengths)
        if self.N == 0:
            raise ValueError("doc_lengths is empty; BM25 requires document stats.")

        # Precompute document frequency (df) per term
        # index[term] is expected to be a dict {docid: tf}
        self.df = {term: len(postings) for term, postings in index.items()}

        # Average document length
        self.avgdl = sum(doc_lengths.values()) / self.N

    def bm25(self, tf, df, dl):
        """
        Compute BM25 score for a single term contribution.

        Args:
            tf: term frequency in this document
            df: document frequency of the term
            dl: document length of this document
        """
        # Standard BM25 with +1 inside the log to avoid negative underflows on small corpora
        idf = math.log((self.N - df + 0.5) / (df + 0.5) + 1.0)
        numerator = tf * (self.k1 + 1.0)
        denominator = tf + self.k1 * (1.0 - self.b + self.b * (dl / self.avgdl))
        return idf * (numerator / denominator)

    def score(self, query):
        """
        Compute BM25 scores for all documents that contain at least one query term.

        Args:
            query: raw query string (space-separated terms)

        Returns:
            A list of (docid, score) sorted by score descending.
        """
        q_terms = query.lower().split()
        scores = defaultdict(float)

        for term in q_terms:
            postings = self.index.get(term)
            if not postings:
                continue
            df = self.df.get(term, 0)
            if df == 0:
                continue
            for docid, tf in postings.items():
                dl = self.doc_lengths[docid]
                scores[docid] += self.bm25(tf, df, dl)

        # sort by BM25 score descending
        return sorted(scores.items(), key=lambda x: x[1], reverse=True)


if __name__ == "__main__":
    # Smoke test: load saved index and doc_lengths, then rank a sample query.
    index = load_index(INDEX_PATH)
    doc_lengths = load_doc_lengths(DOC_LENGTHS_PATH)
    ranker = Ranker(index, doc_lengths)

    for q in ["communication", "machine learning", "u.s policy", "3.14 math"]:
        results = ranker.score(q)[:10]
        print(f"\nQuery: {q}")
        for docid, score in results:
            print(f"  {docid}\t{score:.3f}")
