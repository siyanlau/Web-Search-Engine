"""
engine/indexer.py

Builds an inverted index from parsed documents and writes it to disk
in *blocked binary format* (v0.4). This replaces the earlier pickle-based
index for scalability and I/O efficiency.

Output files:
    - index.postings : binary file storing blocked posting lists
    - index.lexicon  : term -> metadata (offset, df, nblocks)
    - doc_lengths.pkl: document length mapping (written by parser)
"""

from collections import defaultdict
from engine.paths import POSTINGS_PATH, LEXICON_PATH, MARCO_TSV_PATH
from engine.listio import ListWriter
from engine.lexicon import Lexicon


class Indexer:
    """
    In-memory inverted index builder.
    Maintains a temporary dictionary mapping:
        term -> {docid: term_frequency}

    After building, use save_to_disk() to serialize the index into
    a blocked on-disk format with a corresponding lexicon file.
    """

    def __init__(self):
        # Defaultdict nesting ensures new docid keys auto-initialize to 0
        self.index = defaultdict(lambda: defaultdict(int))

    def build_inverted_index(self, docs: dict[int, list[str]]):
        """
        Construct an inverted index from tokenized documents.

        Args:
            docs: dict mapping docID -> list of tokens

        Returns:
            dict[str, dict[int, int]] : term -> {docid: frequency}
        """
        for docid, tokens in docs.items():
            for t in tokens:
                self.index[t][docid] += 1
        return self.index

    def get_postings(self, term: str):
        """
        Retrieve the posting list for a given term from in-memory index.
        Returns an empty dict if term not found.
        """
        return self.index.get(term, {})

    def save_to_disk(self):
        """
        Serialize the current in-memory index to disk in blocked binary format.

        For each term:
            - Writes its postings (sorted by docid) into index.postings in blocks
            - Records its offset, document frequency, and number of blocks
              into index.lexicon

        Files written:
            - POSTINGS_PATH (binary)
            - LEXICON_PATH (pickle)
        """
        writer = ListWriter(POSTINGS_PATH)
        lex = Lexicon()

        for term, postings in self.index.items():
            entry = writer.add_term(term, postings)
            lex.add(term, entry)

        writer.close()
        lex.save(LEXICON_PATH)
        print(f"[Indexer] Wrote postings → {POSTINGS_PATH}")
        print(f"[Indexer] Wrote lexicon  → {LEXICON_PATH}")


# -------------------------------
# Optional manual test / smoke run
# -------------------------------
if __name__ == "__main__":
    from engine.parser import Parser
    from engine.paths import MARCO_TSV_PATH

    print("[Indexer] Building index from", MARCO_TSV_PATH)
    parser = Parser()
    docs, _ = parser.parse_docs(MARCO_TSV_PATH, limit=30000)

    indexer = Indexer()
    indexer.build_inverted_index(docs)
    indexer.save_to_disk()

    # Quick sanity check: print one term’s postings length
    sample_term = "communication"
    postings = indexer.get_postings(sample_term)
    print(f"Sample postings for '{sample_term}': {len(postings)} docs")
