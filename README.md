# MiniCrawler v0.2

**What's new vs v0.1**
- CSV adds **status** and **depth** (`ts_iso,url,status,depth`).
- **Content gating**: suffix blacklist; only parse when `Content-Type` contains `text/html`; body size cap.
- **robots.txt**: cached, checked **before enqueue**.
- **Password/login walls**: classify 401/403 and login redirects/forms; skip parsing.
- **<base> tag**: resolve relative links against `<base href>` if present.
- Clean module structure so we can add more features incrementally.

## Usage
```bash
python -m crawl.main --seeds-file seeds.txt --out crawl.csv   --max-pages 200 --max-depth 1 --timeout 5 --user-agent "MiniCrawler/0.2"
```