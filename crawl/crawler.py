
import csv, time
from collections import deque
from urllib.parse import urldefrag

from .fetch import fetch_url
from .parse import LinkExtractor

def crawl(seeds, out_csv, max_pages, max_depth, timeout, ua):
    visited = set()
    q = deque()
    for s in seeds:
        s = s.strip()
        if not s:
            continue
        s, _ = urldefrag(s)
        q.append((s, 0))

    outfh = open(out_csv, "w", newline="", encoding="utf-8")
    writer = csv.writer(outfh)
    writer.writerow(["ts_iso", "url", "status", "depth"])

    fetched = 0
    try:
        while q and fetched < max_pages:
            url, depth = q.popleft()
            res = fetch_url(url, timeout, ua)
            final_url = res["final_url"]
            status = res["status"]
            body = res["body"]
            ts_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            writer.writerow([ts_iso, final_url, status, depth])
            fetched += 1

            if final_url in visited:
                continue
            visited.add(final_url)

            if not body or depth >= max_depth:
                continue

            # parse links
            try:
                parser = LinkExtractor(final_url)
                text = body.decode("utf-8", errors="replace")
                parser.feed(text)
                for child in parser.links:
                    if child not in visited:
                        q.append((child, depth + 1))
            except Exception:
                continue
    finally:
        outfh.close()
