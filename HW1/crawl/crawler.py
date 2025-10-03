import csv
import time
import random
import math
import heapq
import threading
from urllib.parse import urldefrag, urlparse

from .fetch import fetch_url
from .parse import LinkExtractor
from .robots import RobotCache
from .helpers import get_domain, get_superdomain

# =========================
# Configurable constants
# =========================
BINARY_SUFFIXES = (
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico",
    ".pdf", ".zip", ".tar", ".gz", ".tgz", ".bz2", ".xz", ".rar", ".7z",
    ".mp3", ".wav", ".flac", ".mp4", ".avi", ".mov", ".mkv", ".webm",
    ".css", ".js", ".woff", ".woff2", ".ttf", ".eot"
)

SUPERDOMAIN_WEIGHT = 0.1
MAX_KEEP = 100
OVERSAMPLE = 200
NUM_WORKERS = 32   # number of threads to run
FRONTIER_CAP = 10000
FRONTIER_KEEP = 2000
# =========================

def _looks_binary_by_suffix(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in BINARY_SUFFIXES)
# note that MIME type check is done in fetch.py, not here

def _compute_priority(domain_before: int, super_before: int, super_w: float = SUPERDOMAIN_WEIGHT):
    page_score = 1.0 / math.log2(2.0 + float(domain_before))
    super_score = super_w / math.log2(2.0 + float(super_before))
    total_priority = page_score + super_score
    return page_score, super_score, total_priority

# Locks for multithreading
frontier_lock = threading.Lock()
state_lock = threading.Lock()

# === Worker function (multi-threaded, lazy robots) ===
def worker(worker_id, frontier, visited, in_frontier, pages_per_domain, pages_per_superdomain,
           robots, writer, max_pages, max_depth, timeout, ua, fetched_state,
           total_bytes, error_counts):  # [ADDED] new shared stats
    seq = 0

    while True:
        # Try to grab a URL from the frontier
        with frontier_lock:
            if fetched_state[0] >= max_pages:
                return
            if not frontier:
                url = None
            else:
                neg_prio, depth, _, url, prio_at_pop = heapq.heappop(frontier)
                print(f"[POP] [W{worker_id}] selected_prio={prio_at_pop:.3f} url={url}")
                in_frontier.discard(url)

        if url is None:
            time.sleep(0.1)
            continue  # retry loop

        # --- Lazy robots check here ---
        if not robots.can_fetch(url):
            print(f"[ROBOTS-LAZY] [W{worker_id}] disallow {url}")
            continue

        # --- Fetch outside lock ---
        res = fetch_url(url, timeout, ua)
        final_url = res["final_url"]
        status = res["status"]
        body = res["body"]
        
        try:
            status = int(status)
        except Exception:
            status = 0

        # --- State + logging critical section ---
        ts_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        size_bytes = len(body) if body else 0
        domain = get_domain(final_url)
        superdomain = get_superdomain(final_url)

        with state_lock:
            if final_url in visited:
                print(f"[SKIP DUP] {final_url}")
                continue

            domain_before = pages_per_domain.get(domain, 0)
            super_before = pages_per_superdomain.get(superdomain, 0)

            page_score, super_score, total_priority = _compute_priority(domain_before, super_before)

            writer.writerow([
                ts_iso, final_url, status, depth, size_bytes,
                domain, superdomain,
                domain_before, super_before,
                f"{page_score:.3f}", f"{super_score:.3f}", f"{total_priority:.3f}",
                f"{prio_at_pop:.3f}"
            ])
            fetched_state[0] += 1
            visited.add(final_url)
            pages_per_domain[domain] = domain_before + 1
            pages_per_superdomain[superdomain] = super_before + 1

            # [ADDED] bump shared stats
            # Count total bytes fetched and error statuses for end-of-run console summary
            total_bytes[0] += size_bytes
            if status >= 400:
                error_counts[status] = error_counts.get(status, 0) + 1

        print(f"[FETCH] [W{worker_id}] {status} {final_url} depth={depth} bytes={size_bytes} "
            f"domain={domain}({domain_before}) super={superdomain}({super_before}) "
            f"scores=({page_score:.3f},{super_score:.3f}) total={total_priority:.3f}")


        # --- Skip children if body/status/depth not suitable ---
        if (not body) or (depth >= max_depth) or (status >= 400):
            # status >= 400 includes a lot of issues, e.g. password protected pages. We just toss them all
            continue

        # --- Parse and enqueue children ---
        try:
            parser = LinkExtractor(final_url)
            text = body.decode("utf-8", errors="replace")
            parser.feed(text)
            links = parser.links
            print(f"[PARSE] [W{worker_id}] found {len(links)} links at {final_url}")

            # Step 1: oversample + cap
            original_n = len(links)
            if original_n > MAX_KEEP:
                sample_idx = random.sample(range(original_n), min(OVERSAMPLE, original_n))
                links = [links[i] for i in sample_idx]
                print(f"[CAP] [W{worker_id}] page had {original_n} links → sampled {len(links)} candidates")

            # Step 2: suffix filter
            filtered = []
            for u in links:
                if _looks_binary_by_suffix(u):
                    print(f"[SKIP BIN] [W{worker_id}] {u}")
                    continue
                filtered.append(u)

            # Step 3: enqueue children without robots check (lazy)
            to_enqueue = []
            for child in filtered:
                if child in visited or child in in_frontier:
                    continue
                if "cgi" in child.lower():
                    print(f"[SKIP CGI] [W{worker_id}] {child}") # Skip CGI scripts
                    continue
                cd = get_domain(child)
                csd = get_superdomain(child)
                cd_before = pages_per_domain.get(cd, 0)
                csd_before = pages_per_superdomain.get(csd, 0)
                _, _, tp = _compute_priority(cd_before, csd_before)
                to_enqueue.append((-tp, depth + 1, seq, child, tp))
                seq += 1

            # Step 4: bulk push inside lock
            if to_enqueue:
                with frontier_lock:
                    accepted = 0
                    for item in to_enqueue:
                        _, _, _, child, _ = item
                        if child not in visited and child not in in_frontier:
                            heapq.heappush(frontier, item)
                            in_frontier.add(child)
                            accepted += 1
                            if accepted >= MAX_KEEP:
                                break
                print(f"[ENQUEUE] [W{worker_id}] accepted={accepted}, frontier_size={len(frontier)}")
            
            # if PQ blows up in size, we trim it down
            if len(frontier) > FRONTIER_CAP:
                # Keep only the top 2000 items by priority
                frontier[:] = heapq.nsmallest(FRONTIER_KEEP, frontier, key=lambda x: x[0])
                heapq.heapify(frontier)
                print(f"[FRONTIER CAP] trimmed to {FRONTIER_KEEP}, new frontier_size={len(frontier)}")

        except Exception as e:
            print(f"[PARSE ERROR] [W{worker_id}] {e}")


def crawl(seeds, out_csv, max_pages, max_depth, timeout, ua):
    visited = set()
    in_frontier = set()
    pages_per_domain = {}
    pages_per_superdomain = {}

    robots = RobotCache(user_agent=ua, timeout=timeout)

    frontier = []
    
    seq = 0
    for s in seeds:
        s = (s or "").strip()
        if not s:
            continue
        s, _ = urldefrag(s)
        if not robots.can_fetch(s):  # still check seeds upfront
            print(f"[SEED SKIP] robots disallow {s}")
            continue
        if s in visited or s in in_frontier:
            continue
        d = get_domain(s)
        sd = get_superdomain(s)
        d_before = pages_per_domain.get(d, 0)
        sd_before = pages_per_superdomain.get(sd, 0)
        _, _, prio = _compute_priority(d_before, sd_before)
        heapq.heappush(frontier, (-prio, 0, seq, s, prio))
        in_frontier.add(s)
        print(f"[SEED] push depth=0 prio={prio:.3f} {s}")
        seq += 1

    outfh = open(out_csv, "w", newline="", encoding="utf-8")
    writer = csv.writer(outfh)
    writer.writerow([
    "ts_iso", "url", "status", "depth", "bytes",
    "domain", "superdomain",
    "domain_count_before", "super_count_before",
    "page_score", "super_score", "total_priority",
    "priority_at_pop"
    ])

    fetched_state = [0]  # mutable wrapper to share count
    total_bytes = [0]    # [ADDED] total bytes across all pages
    error_counts = {}    # [ADDED] status>=400 tallies
    start_time = time.time()  # [ADDED] for elapsed seconds

    try:
        threads = []
        for i in range(NUM_WORKERS):
            t = threading.Thread(target=worker, args=(
                i, 
                frontier, visited, in_frontier,
                pages_per_domain, pages_per_superdomain,
                robots, writer,
                max_pages, max_depth, timeout, ua,
                fetched_state,
                total_bytes, error_counts  # [ADDED]
            ))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        # [ADDED] Console summary (not written to CSV)
        elapsed = time.time() - start_time
        print("\n---- SUMMARY ----")
        print(f"Pages crawled:       {fetched_state[0]}")
        print(f"Elapsed seconds:     {elapsed:.2f}")
        print(f"Total bytes:         {total_bytes[0]}")
        print(f"Unique domains:      {len(pages_per_domain)}")
        print(f"Unique superdomains: {len(pages_per_superdomain)}")
        if error_counts:
            for code in sorted(error_counts):
                print(f"HTTP {code} errors:   {error_counts[code]}")

    finally:
        outfh.close()
