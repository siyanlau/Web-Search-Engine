import struct
from collections import defaultdict
from typing import Iterator, Tuple, List, Optional

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
        docids = sorted(postings.keys())
        freqs  = [postings[d] for d in docids]

        start_offset = self.file.tell() # where is my cursor within the file
        total_df = len(docids)
        blocks_meta = []   # NEW: per-block metadata for fast seek

        # chunk into blocks
        for i in range(0, total_df, self.block_size):
            chunk_docids = docids[i:i+self.block_size]
            chunk_freqs  = freqs[i:i+self.block_size]
            n = len(chunk_docids)
            last_docid = chunk_docids[-1]

            # record offset of this block before writing
            block_offset = self.file.tell()

            # write header: <n:uint32, last_docid:uint32>
            self.file.write(struct.pack("<II", n, last_docid))

            # write docids and freqs as uint32 arrays (raw v0.4)
            self.file.write(struct.pack(f"<{n}I", *chunk_docids))
            self.file.write(struct.pack(f"<{n}I", *chunk_freqs))

            # NEW: bytes written for docids/freqs (for future varbyte)
            doc_bytes  = 4 * n
            freq_bytes = 4 * n

            blocks_meta.append({
                "offset": block_offset,   # absolute file offset at block header
                "n": n,
                "last_docid": last_docid,
                "doc_bytes": doc_bytes,
                "freq_bytes": freq_bytes,
            })

        entry = {
            "offset": start_offset,      # first block offset for this term
            "df": total_df,
            "nblocks": len(blocks_meta),
            "blocks": blocks_meta,       # NEW: block directory
        }
        return entry

    def close(self):
        size = self.file.tell()
        self.file.close()
        print(f"ListWriter: wrote {size} bytes to {self.filepath}")



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
        
    def iter_blocks(self, entry: dict) -> Iterator[Tuple[int, List[int], List[int]]]:
        """
        Yield blocks for this term one-by-one.
        Each yield returns (last_docid, docids[], freqs[]).

        If 'blocks' metadata is present in entry, use it (fast path).
        Otherwise, fall back to linear scan using headers (compatible with older entries).
        """
        df = entry["df"]
        remaining = df

        # fast path: use per-block directory if present
        blocks = entry.get("blocks")
        if blocks:
            for b in blocks:
                self.file.seek(b["offset"])
                # read header to stay robust even if bytes change in the future
                n, last_docid = struct.unpack("<II", self.file.read(8))
                d = list(struct.unpack(f"<{n}I", self.file.read(4*n)))
                f = list(struct.unpack(f"<{n}I", self.file.read(4*n)))
                yield last_docid, d, f
            return

        # fallback: linear scan from the first offset
        self.file.seek(entry["offset"])
        while remaining > 0:
            n, last_docid = struct.unpack("<II", self.file.read(8))
            d = list(struct.unpack(f"<{n}I", self.file.read(4*n)))
            f = list(struct.unpack(f"<{n}I", self.file.read(4*n)))
            yield last_docid, d, f
            remaining -= n

    def seek_block_ge(self, entry: dict, target_docid: int):
        """
        Locate the first block whose last_docid >= target_docid.
        Return a tuple (block_index, last_docid, docids[], freqs[]).
        If not found, return None.

        Uses the 'blocks' directory if present (binary search).
        Falls back to linear scan otherwise.
        """
        blocks = entry.get("blocks")
        if blocks:
            # binary search on last_docid
            lo, hi = 0, len(blocks) - 1
            ans = None
            while lo <= hi:
                mid = (lo + hi) // 2
                if blocks[mid]["last_docid"] >= target_docid:
                    ans = mid
                    hi = mid - 1
                else:
                    lo = mid + 1
            if ans is None:
                return None

            b = blocks[ans]
            self.file.seek(b["offset"])
            n, last_docid = struct.unpack("<II", self.file.read(8))
            d = list(struct.unpack(f"<{n}I", self.file.read(4*n)))
            f = list(struct.unpack(f"<{n}I", self.file.read(4*n)))
            return ans, last_docid, d, f

        # fallback: linear scan (no blocks[] present)
        self.file.seek(entry["offset"])
        idx = 0
        remaining = entry["df"]
        while remaining > 0:
            n, last_docid = struct.unpack("<II", self.file.read(8))
            d = list(struct.unpack(f"<{n}I", self.file.read(4*n)))
            f = list(struct.unpack(f"<{n}I", self.file.read(4*n)))
            if last_docid >= target_docid:
                return idx, last_docid, d, f
            remaining -= n
            idx += 1
        return None
