import csv, time
from collections import deque
from urllib.parse import urldefrag, urlparse

from .fetch import fetch_url
from .parse import LinkExtractor
from .robots import RobotCache

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

                accepted = 0
                for child in links:
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

                print(f"[ENQUEUE] accepted={accepted}, queue_size={len(q)}")

            except Exception as e:
                print(f"[PARSE ERROR] {e}")
    finally:
        outfh.close()
