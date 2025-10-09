import re
import html
from ftfy import fix_text
from engine.utils import write_doc_lengths
from engine.paths import DOC_LENGTHS_PATH

class Parser:
    """
    Robust parser for MS MARCO-style TSV files.
    Uses ftfy + html to clean malformed text.

    What it does:
    - Turns dirty html code into regular chars
    - Funny looking chars like â\x80\x93 into regular chars
    - Tokenization
    - Removes non-English words
    - U.S. -> u.s    3-14 -> 3-14  This is not perfect but should suffice for our purpose

    Methods:
        parse_docs(path: str, limit: int | None = None):
            Returns:
                docs: dict[int, list[str]]
                doc_lengths: dict[int, int]
    """
    
    def __init__(self):
        pass
    
    
    def parse_docs(self, path: str, limit: int | None = None):
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
                tokens = re.findall(r"[a-z0-9]+(?:[.-][a-z0-9]+)*", text.lower()) # keep U.S., 3.14, etc whole words
                if not tokens:
                    continue

                docs[docid] = tokens
                doc_lengths[docid] = len(tokens)
        print(f"Loaded {len(docs)} docs")
        
        # now we have each document's length, and it's not gonna change, we write it to disk
        # may need to modify (inplement compression, though i doubt this will be a bottleneck)
        write_doc_lengths(doc_lengths, DOC_LENGTHS_PATH)

        return docs, doc_lengths


if __name__ == "__main__":
    parser = Parser()
    docs, lens = parser.parse_docs("data/marco_small.tsv")
    for docid, toks in list(docs.items())[:10]:
        print(docid, toks)
