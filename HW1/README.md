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

URL: http://cs.nyu.edu/index.html
  Domain:      nyu.edu
  Superdomain: edu
----------------------------------------
URL: https://www.guardian.co.uk/news
  Domain:      guardian.co.uk
  Superdomain: uk
----------------------------------------
URL: http://subdomain.example.com/page
  Domain:      example.com
  Superdomain: com
----------------------------------------
URL: https://localhost:8080/
  Domain:      localhost
  Superdomain: localhost
----------------------------------------
URL: http://weirdtld.technology/path
  Domain:      weirdtld.technology
  Superdomain: technology
----------------------------------------
URL: http://bbc.co.uk
  Domain:      bbc.co.uk
  Superdomain: uk
  Domain:      bbc.co.uk
  Domain:      bbc.co.uk
  Domain:      bbc.co.uk
  Superdomain: uk
----------------------------------------
URL: http://ox.ac.uk
  Domain:      ox.ac.uk
  Superdomain: uk
----------------------------------------