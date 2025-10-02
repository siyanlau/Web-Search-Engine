import csv, time, random
from collections import deque
from urllib.parse import urldefrag, urlparse

from .fetch import fetch_url
from .parse import LinkExtractor
from .robots import RobotCache

# suffix blacklist (for enqueue-time gating)
BINARY_SUFFIXES = (
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico",
    ".pdf", ".zip", ".tar", ".gz", ".tgz", ".bz2", ".xz", ".rar", ".7z",
    ".mp3", ".wav", ".flac", ".mp4", ".avi", ".mov", ".mkv", ".webm",
    ".css", ".js", ".woff", ".woff2", ".ttf", ".eot"
)

def _looks_binary_by_suffix(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in BINARY_SUFFIXES)

def crawl(seeds, out_csv, max_pages, max_depth, timeout, ua):
    visited = set()
    q = deque()

    robots = RobotCache(user_agent=ua, timeout=timeout)

    # seed enqueue
    for s in seeds:
        s = s.strip()
        if not s:
            continue
        s, _ = urldefrag(s)
        if robots.can_fetch(s):
            q.append((s, 0))
        else:
            print(f"[SEED SKIP] robots disallow {s}")

    outfh = open(out_csv, "w", newline="", encoding="utf-8")
    writer = csv.writer(outfh)
    writer.writerow(["ts_iso", "url", "status", "depth", "bytes"])

    fetched = 0
    try:
        while q and fetched < max_pages:
            url, depth = q.popleft()
            res = fetch_url(url, timeout, ua)
            final_url = res["final_url"]
            status = res["status"]
            body = res["body"]

            ts_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            size_bytes = len(body) if body else 0
            writer.writerow([ts_iso, final_url, status, depth, size_bytes])
            fetched += 1

            print(f"[FETCH] {status} {final_url} depth={depth}")

            if final_url in visited:
                continue
            visited.add(final_url)

            if not body or depth >= max_depth:
                continue

            # parse + enqueue
            try:
                parser = LinkExtractor(final_url)
                text = body.decode("utf-8", errors="replace")
                print(f"[DEBUG] first 200 chars of body:\n{text[:200]!r}")
                parser.feed(text)
                links = parser.links
                print(f"[PARSE] found {len(links)} links at {final_url}")

                # --- Step 1: cap with random oversampling ---
                max_keep = 100
                oversample = 200
                if len(links) > max_keep:
                    sample_idx = random.sample(
                        range(len(links)), min(oversample, len(links))
                    )
                    links = [links[i] for i in sample_idx]
                    print(f"[CAP] page had {len(parser.links)} links → sampled {len(links)} candidates")
                # --------------------------------------------

                # --- Step 2: filter suffixes only on sampled set ---
                filtered = []
                for u in links:
                    if _looks_binary_by_suffix(u):
                        print(f"[SKIP BIN] {u}")
                        continue
                    filtered.append(u)
                # ---------------------------------------------------

                # --- Step 3: robots + enqueue ---
                accepted = 0
                for child in filtered:
                    if child in visited:
                        continue

                    # # optional: same-host restriction for debug
                    # if urlparse(child).netloc != urlparse(final_url).netloc:
                    #     print(f"[SKIP EXT] {child}")
                    #     continue

                    allowed = robots.can_fetch(child)
                    if not allowed:
                        print(f"[ROBOTS] disallow {child}")
                        continue
                    q.append((child, depth + 1))
                    accepted += 1
                    if accepted >= max_keep:
                        break

                print(f"[ENQUEUE] accepted={accepted}, queue_size={len(q)}")

            except Exception as e:
                print(f"[PARSE ERROR] {e}")
    finally:
        outfh.close()
