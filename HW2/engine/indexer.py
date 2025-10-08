from collections import defaultdict
from engine.paths import INDEX_PATH, MARCO_TSV_PATH
from engine.utils import write_index, load_index  # single source of truth for IO


class Indexer:
    """
    Inverted index builder.
    Maintains an in-memory term -> {docid: freq} mapping.
    """

    def __init__(self):
        self.index = defaultdict(lambda: defaultdict(int))

    def build_inverted_index(self, docs):
        """
        Given docs: dict[int, list[str]],
        build an inverted index: term -> {docid: freq}
        """
        for docid, tokens in docs.items():
            for t in tokens:
                self.index[t][docid] += 1
        return self.index 

    def get_postings(self, term):
        """
        Returns posting list for a given term as dict {docid: freq}.
        """
        return self.index.get(term, {})
    
    def save_to_disk(self, path: str = INDEX_PATH):
        """
        Persist the current inverted index to disk.
        Delegates to engine.utils to keep IO concerns centralized.
        """
        # Convert nested defaultdicts to plain dicts for portability
        data = {term: dict(postings) for term, postings in self.index.items()}
        write_index(data, path)
        print(f"Indexer: index saved to {path}")
        
        
    @classmethod
    def load_from_disk(cls, path: str = INDEX_PATH):
        """
        Load an inverted index from disk and return a ready-to-use Indexer.
        Delegates file IO to engine.utils.
        """
        data = load_index(path)  # data: dict[str, dict[int,int]]
        idx = cls()
        # Restore nested defaultdict structure
        idx.index = defaultdict(lambda: defaultdict(int), {
            term: defaultdict(int, postings) for term, postings in data.items()
        })
        print(f"Indexer: index loaded from {path}")
        return idx
        
if __name__ == "__main__":
    # Minimal end-to-end smoke test (run from project root: `python -m engine.indexer`)
    from engine.parser import Parser
    parser = Parser()
    docs, _ = parser.parse_docs(MARCO_TSV_PATH, limit=1000)
    indexer = Indexer()
    indexer.build_inverted_index(docs)
    indexer.save_to_disk(INDEX_PATH)

    loaded = Indexer.load_from_disk(INDEX_PATH)
    print("Sample postings for 'communication':", list(loaded.get_postings("communication").items())[:5])