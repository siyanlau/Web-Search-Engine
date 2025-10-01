
import urllib.robotparser
from urllib.parse import urlparse

class RobotCache:
    def __init__(self, user_agent: str, timeout: float = 5.0):
        self.user_agent = user_agent
        self.timeout = timeout
        self.cache = {}

    def _fetch_parser(self, root: str):
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(root)
        try:
            rp.read()
        except Exception:
            pass
        return rp

    def can_fetch(self, url: str) -> bool:
        host = urlparse(url).netloc
        if not host:
            return False
        robots_url = f"https://{host}/robots.txt"
        rp = self.cache.get(robots_url)
        if rp is None:
            rp = self._fetch_parser(robots_url)
            self.cache[robots_url] = rp
        try:
            return rp.can_fetch(self.user_agent, url)
        except Exception:
            return True
