# engine/parser.py
import re

def parse_docs(path: str, limit: int | None = None):
    
    """
    Parse a TSV file (<docid>\\t<text>) into tokenized documents.

    Parameters
    ----------
    path : str
        Path to the input file (UTF-8 encoded).
    limit : int | None
        Optional limit on number of documents to read.

    Returns
    -------
    docs : dict[int, list[str]]
        Mapping of docID -> list of lowercase tokens.
    doc_lengths : dict[int, int]
        Mapping of docID -> number of tokens in that document.
    """
    
    docs = {}
    doc_lengths = {}
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if limit is not None and i >= limit:
                break
            line = line.strip()
            if not line:
                continue
            # allow \t or multiple space keys
            parts = re.split(r"\s+", line, maxsplit=1)
            if len(parts) != 2:
                continue
            docid_str, text = parts
            try:
                docid = int(docid_str)
            except ValueError:
                continue
            tokens = re.findall(r"[a-z0-9]+", text.lower())
            if not tokens:
                continue
            docs[docid] = tokens
            doc_lengths[docid] = len(tokens)
    return docs, doc_lengths

if __name__ == "__main__":
    docs, lens = parse_docs("data/toy.txt")
    print(f"Loaded {len(docs)} docs")
    for docid, toks in list(docs.items())[5:7]:
        print(docid, toks)