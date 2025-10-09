import struct
from collections import defaultdict

BLOCK_SIZE = 128  # adjustable

class ListWriter:
    """
    Writes term postings into a binary file in blocked format (no compression).
    Each block stores:
      uint32 n_in_block
      uint32 last_docid
      uint32[n_in_block] docids
      uint32[n_in_block] freqs
    """
    def __init__(self, filepath, block_size=BLOCK_SIZE):
        self.filepath = filepath
        self.block_size = block_size
        self.file = open(filepath, "wb")
        self.offset = 0  # byte offset counter

    def add_term(self, term, postings):
        """
        Write postings for a single term.
        postings: dict {docid: freq}
        Returns summary: (offset, df, nblocks)
        """
        docids = sorted(postings.keys())
        freqs = [postings[d] for d in docids]
        df = len(docids)
        nblocks = 0
        start_offset = self.offset

        for i in range(0, df, self.block_size):
            block_docids = docids[i:i + self.block_size]
            block_freqs = freqs[i:i + self.block_size]
            n = len(block_docids)
            last_docid = block_docids[-1]

            # pack header
            block = struct.pack("<II", n, last_docid)
            # pack postings
            block += struct.pack(f"<{n}I", *block_docids)
            block += struct.pack(f"<{n}I", *block_freqs)

            self.file.write(block)
            nblocks += 1
            self.offset += len(block)

        return {"offset": start_offset, "df": df, "nblocks": nblocks}

    def close(self):
        self.file.close()
        print(f"ListWriter: wrote {self.offset} bytes to {self.filepath}")


class ListReader:
    """
    Reads postings from blocked binary file.
    """
    def __init__(self, filepath):
        self.filepath = filepath
        self.file = open(filepath, "rb")

    def read_postings(self, lexicon_entry):
        """
        Read full postings list given lexicon entry:
          {"offset": int, "df": int, "nblocks": int}
        """
        self.file.seek(lexicon_entry["offset"])
        docids, freqs = [], []
        for _ in range(lexicon_entry["nblocks"]):
            n, last_docid = struct.unpack("<II", self.file.read(8))
            d = struct.unpack(f"<{n}I", self.file.read(4 * n))
            f = struct.unpack(f"<{n}I", self.file.read(4 * n))
            docids.extend(d)
            freqs.extend(f)
        return docids, freqs

    def close(self):
        self.file.close()
