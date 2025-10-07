from collections import defaultdict

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
    

if __name__ == "__main__":
    from engine.parser import Parser
    parser = Parser()
    docs, _ = parser.parse_docs("data/marco_tiny.tsv")
    indexer = Indexer()
    index = indexer.build_inverted_index(docs)
    print(f"Indexed {len(docs)} docs, {len(index)} terms")
    # sample output
    for term, postings in list(index.items())[:10]:
        print(term, list(postings.items())[:50])
    postings = indexer.get_postings("communication")
    print(postings)
