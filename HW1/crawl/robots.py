import urllib.request, urllib.robotparser

class RobotCache:
    def __init__(self, user_agent: str, timeout: float = 5.0):
        self.user_agent = user_agent
        self.timeout = timeout
        self.cache = {}

    def _fetch_parser(self, root: str):
        rp = urllib.robotparser.RobotFileParser()
        try:
            req = urllib.request.Request(root, headers={"User-Agent": self.user_agent})
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                text = resp.read().decode("utf-8", errors="ignore")
            rp.parse(text.splitlines())
        except Exception as e:
            # fallback: treat as allow-all if we can’t fetch
            rp.parse(["User-agent: *", "Disallow:"])  
        return rp

    def can_fetch(self, url: str) -> bool:
        from urllib.parse import urlparse
        host = urlparse(url).netloc
        if not host:
            return False
        robots_url = f"https://{host}/robots.txt"
        rp = self.cache.get(robots_url)
        if rp is None:
            rp = self._fetch_parser(robots_url)
            self.cache[robots_url] = rp
        return rp.can_fetch(self.user_agent, url)
