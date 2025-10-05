# engine/parser.py
import re
import html
from ftfy import fix_text

def parse_docs(path: str, limit: int | None = None):
    """
    Robust parser for MS MARCO-style TSV files.
    Uses ftfy + html to clean malformed text.
    
    What it does:
    - Turns dirty html code into regular chars
    - Funny looking chars like â\x80\x93 into regular chars
    - Tokenization
    - Removes non-English words
    
    Returns:
        docs: dict[int, list[str]]
        doc_lengths: dict[int, int]
    """
    docs = {}
    doc_lengths = {}

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for i, line in enumerate(f):
            if limit is not None and i >= limit:
                break
            parts = line.rstrip("\n").split("\t", 1)
            if len(parts) != 2:
                continue

            docid_str, text = parts
            try:
                docid = int(docid_str)
            except ValueError:
                continue

            # Clean weird encodings and HTML
            text = fix_text(html.unescape(text))
            tokens = re.findall(r"[a-z0-9]+", text.lower())
            if not tokens:
                continue

            docs[docid] = tokens
            doc_lengths[docid] = len(tokens)

    return docs, doc_lengths


if __name__ == "__main__":
    docs, lens = parse_docs("data/marco_tiny.tsv")
    print(f"Loaded {len(docs)} docs")
    for docid, toks in list(docs.items())[:10]:
        print(docid, toks)