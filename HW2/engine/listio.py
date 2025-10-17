from __future__ import annotations
import struct
from collections import defaultdict
from typing import Iterator, Tuple, List, Optional


BLOCK_SIZE = 128  # adjustable

class ListWriter:
    """
    Writes term postings into a binary file in blocked format. 
    Each block stores:
      uint32 n_in_block
      uint32 last_docid
      uint32[n_in_block] docids
      uint32[n_in_block] freqs
    """
    def __init__(self, filepath, block_size=BLOCK_SIZE, codec: str = "raw"):
        self.filepath = filepath
        self.block_size = block_size
        self.codec = codec.lower()
        self.file = open(filepath, "wb")
        self.offset = 0  # byte offset counter

    def add_term(self, term: str, postings: dict[int, int]):
        """
        Write postings for a single term in blocked binary format.
        postings: {docid: tf}
        Returns a lexicon entry that includes per-block metadata:
          { 'offset': int, 'df': int, 'nblocks': int,
            'blocks': [ { 'offset': int, 'doc_bytes': int, 'freq_bytes': int, 'last_docid': int }, ... ],
            'codec': 'raw'|'varbyte'
          }
        """
        # Ensure sorted by docid
        items = sorted(postings.items(), key=lambda x: x[0])
        df = len(items)
        start_offset = self.file.tell()

        blocks_meta = []
        prev_last = 0  # base for the first block

        # chunk by block_size
        for i in range(0, df, self.block_size):
            chunk = items[i:i+self.block_size]
            docids = [d for d, _ in chunk]
            freqs  = [f for _, f in chunk]

            block_offset = self.file.tell()

            if self.codec == "varbyte":
                # encode docs as gaps relative to prev_last, freqs as VB
                doc_bytes = VarByteCodec.encode_docids(docids, base=prev_last)
                freq_bytes = VarByteCodec.encode_freqs(freqs)
                # write both slices back-to-back: [doc_bytes][freq_bytes]
                self.file.write(doc_bytes)
                self.file.write(freq_bytes)
                bytes_docs = len(doc_bytes)
                bytes_freq = len(freq_bytes)
            else:
                # RAW fallback (exactly what you used to do; adjust if needed)
                # Here we store docids and freqs as 4-byte little-endian ints one after another.
                import struct
                # docs
                for d in docids:
                    self.file.write(struct.pack("<I", d))
                # freqs
                for f in freqs:
                    self.file.write(struct.pack("<I", f))
                bytes_docs = 4 * len(docids)
                bytes_freq = 4 * len(freqs)

            last_docid = docids[-1]
            blocks_meta.append({
                "offset": block_offset,
                "doc_bytes": bytes_docs,
                "freq_bytes": bytes_freq,
                "last_docid": last_docid,
            })
            prev_last = last_docid

        entry = {
            "offset": start_offset,
            "df": df,
            "nblocks": len(blocks_meta),
            "blocks": blocks_meta,
            "codec": self.codec,  # record for reader convenience
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
    def __init__(self, filepath, codec: str = "auto"):
        self.filepath = filepath
        self.file = open(filepath, "rb")
        self.codec = codec.lower() # 'auto' means defer to entry['codec'] if present

    def _entry_codec(self, entry: dict) -> str:
        # helper: decide codec for a given entry
        if self.codec != "auto":
            return self.codec
        return entry.get("codec", "raw").lower()

    def read_postings(self, entry: dict):
        """
        Return full postings as (docids: List[int], freqs: List[int]) for the given lexicon entry.
        """
        docids_all: List[int] = []
        freqs_all: List[int] = []

        codec = self._entry_codec(entry)
        prev_last = 0  # base for first block

        for b in entry["blocks"]:
            off = b["offset"]
            db = b["doc_bytes"]
            fb = b["freq_bytes"]
            self.file.seek(off)
            docs_buf = self.file.read(db)
            freqs_buf = self.file.read(fb)

            if codec == "varbyte":
                # decode doc gaps -> absolute docids using base=prev_last
                docids = VarByteCodec.decode_docids(docs_buf, base=prev_last)
                freqs = VarByteCodec.decode_freqs(freqs_buf)
            else:
                # RAW fallback: 4-byte little-endian ints
                import struct, math
                nd = db // 4
                nf = fb // 4
                docids = list(struct.unpack("<" + "I"*nd, docs_buf)) if nd else []
                freqs  = list(struct.unpack("<" + "I"*nf, freqs_buf))  if nf else []

            docids_all.extend(docids)
            freqs_all.extend(freqs)
            prev_last = b["last_docid"]

        return docids_all, freqs_all

    def close(self):
        self.file.close()
        
    def iter_blocks(self, entry: dict):
        """
        Yield per-block tuples: (last_docid, docids[], freqs[]).
        """
        codec = self._entry_codec(entry)
        prev_last = 0
        for b in entry["blocks"]:
            off = b["offset"]
            db = b["doc_bytes"]
            fb = b["freq_bytes"]
            self.file.seek(off)
            docs_buf = self.file.read(db)
            freqs_buf = self.file.read(fb)
            if codec == "varbyte":
                docids = VarByteCodec.decode_docids(docs_buf, base=prev_last)
                freqs  = VarByteCodec.decode_freqs(freqs_buf)
            else:
                import struct
                nd = db // 4
                nf = fb // 4
                docids = list(struct.unpack("<" + "I"*nd, docs_buf)) if nd else []
                freqs  = list(struct.unpack("<" + "I"*nf, freqs_buf)) if nf else []
            yield (b["last_docid"], docids, freqs)
            prev_last = b["last_docid"]

    def seek_block_ge(self, entry: dict, target_docid: int):
        """
        Locate the first block whose last_docid >= target_docid.
        Return (block_index, last_docid, docids[], freqs[]); or None.
        Works for both 'raw' and 'varbyte' using the per-block directory.
        """
        blocks = entry.get("blocks")
        codec = self._entry_codec(entry)

        if blocks:
            # binary search on last_docid using directory only
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
            # base = prev block’s last_docid (0 for the first block) — needed for varbyte gaps
            base = blocks[ans - 1]["last_docid"] if ans > 0 else 0

            self.file.seek(b["offset"])
            docs_buf = self.file.read(b["doc_bytes"])
            freqs_buf = self.file.read(b["freq_bytes"])

            if codec == "varbyte":
                docids = VarByteCodec.decode_docids(docs_buf, base=base)
                freqs  = VarByteCodec.decode_freqs(freqs_buf)
            else:
                nd = b["doc_bytes"] // 4
                nf = b["freq_bytes"] // 4
                docids = list(struct.unpack("<" + "I"*nd, docs_buf)) if nd else []
                freqs  = list(struct.unpack("<" + "I"*nf, freqs_buf)) if nf else []

            # defensively ensure lengths match
            if len(docids) != len(freqs):
                raise ValueError(f"Corrupt block: len(docids)={len(docids)} != len(freqs)={len(freqs)}")

            return ans, b["last_docid"], docids, freqs

        # No directory: fall back to linear scan via iter_blocks(), which already handles codecs
        for idx, (last_docid, d, f) in enumerate(self.iter_blocks(entry)):
            if last_docid >= target_docid:
                return idx, last_docid, d, f
        return None


class VarByteCodec:
    """
    VarByte + gap encoding for postings.

    - DocIDs are delta-encoded *within each block*:
        gaps[0] = docids[0] - base   (base = prev block's last_docid; for the first block base=0)
        gaps[i] = docids[i] - docids[i-1]
      Then each gap is VarByte-encoded.

    - Freqs are encoded as VarByte directly (no delta).

    Notes:
    - We set the MSB (0x80) of the *last* byte of each integer to mark termination.
    - All integers must be non-negative.
    """

    @staticmethod
    def _vb_encode_number(x: int, out: bytearray) -> None:
        # Encode non-negative integer x into out (append), MSB marks final byte.
        assert x >= 0
        while True:
            byte = x & 0x7F
            x >>= 7
            if x == 0:
                out.append(byte | 0x80)  # set MSB for last byte
                break
            else:
                out.append(byte)

    @staticmethod
    def _vb_decode_stream(data: bytes) -> List[int]:
        # Decode a VarByte stream into a list of integers.
        res: List[int] = []
        cur = 0
        shift = 0
        for b in data:
            if b & 0x80:           # last byte of this number
                cur |= (b & 0x7F) << shift
                res.append(cur)
                cur = 0
                shift = 0
            else:
                cur |= (b & 0x7F) << shift
                shift += 7
        if shift != 0:
            # malformed stream (dangling partial); ignore for robustness
            pass
        return res

    @classmethod
    def encode_docids(cls, docids: List[int], base: int) -> bytes:
        """
        Encode absolute docids to VarByte of gaps relative to base.
        base is previous block's last_docid (0 for the first block).
        """
        if not docids:
            return b""
        out = bytearray()
        prev = base
        for d in docids:
            gap = d - prev
            # defensive
            if gap < 0:
                raise ValueError(f"Non-monotonic docid sequence: {docids}")
            cls._vb_encode_number(gap, out)
            prev = d
        return bytes(out)

    @classmethod
    def decode_docids(cls, data: bytes, base: int) -> List[int]:
        """
        Decode VarByte gaps back to absolute docids using base.
        """
        gaps = cls._vb_decode_stream(data)
        docids: List[int] = []
        prev = base
        for g in gaps:
            prev = prev + g
            docids.append(prev)
        return docids

    @classmethod
    def encode_freqs(cls, freqs: List[int]) -> bytes:
        out = bytearray()
        for f in freqs:
            if f < 0:
                raise ValueError("Frequency must be non-negative")
            cls._vb_encode_number(f, out)
        return bytes(out)

    @classmethod
    def decode_freqs(cls, data: bytes) -> List[int]:
        return cls._vb_decode_stream(data)