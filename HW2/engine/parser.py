import re
import html
from ftfy import fix_text
from engine.utils import write_doc_lengths
from engine.paths import DOC_LENGTHS_PATH, MARCO_TSV_PATH

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
        (one-off indexing, deprecated pipeline, but gist remains the same)
        parse_docs(path: str, limit: int | None = None):
            Returns:
                docs: dict[int, list[str]]
                doc_lengths: dict[int, int]
    """
    
    def __init__(self):
        pass
    
    
    def parse_docs(self, path: str, limit: int | None = None):
        """
        Deprecated path. This path is used for earlier versions (without merging). 
        Use `tokenize(text)`, `parse_line(line)`, and `iter_docs(path, limit=None)` instead.
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

    def tokenize(self, text: str) -> list[str]:
        """
        Clean and tokenize a raw text string.
        - Fix mojibake (ftfy), unescape HTML entities
        - Lowercase and keep tokens like 'u.s.' or '3.14' as a single token
        - Return [] if nothing remains after tokenization
        """
        text = fix_text(html.unescape(text))
        # Same pattern as parse_docs so behavior is identical
        return re.findall(r"[a-z0-9]+(?:[.-][a-z0-9]+)*", text.lower())
    
    def parse_line(self, line: str):
        """
        Parse a single TSV line into (docid, tokens).
        The TSV is expected to be:  <docid>\t<text...>
        Returns:
            (docid:int, tokens:list[str]) on success
            None if the line is malformed or tokenizes to empty
        """
        parts = line.rstrip("\n").split("\t", 1)
        if len(parts) != 2:
            return None
        docid_str, text = parts
        try:
            docid = int(docid_str)
        except ValueError:
            return None
        tokens = self.tokenize(text)
        if not tokens:
            return None
        return docid, tokens

    def iter_docs(self, path: str, limit: int | None = None):
        """
        Stream (docid, tokens) from a TSV file without side effects.
        Unlike parse_docs(), this does NOT write doc lengths to disk and
        does not accumulate the full corpus in memory.

        Yields:
            (docid:int, tokens:list[str])
        """
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for i, line in enumerate(f):
                if limit is not None and i >= limit:
                    break
                parsed = self.parse_line(line)
                if parsed is None:
                    continue
                yield parsed 


if __name__ == "__main__":
    parser = Parser()
    docs, lens = parser.parse_docs(MARCO_TSV_PATH)
    for docid, toks in list(docs.items())[:10]:
        print(docid, toks)
