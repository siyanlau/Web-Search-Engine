
from urllib.request import Request, build_opener, HTTPRedirectHandler, HTTPSHandler, HTTPHandler
from urllib.error import URLError, HTTPError

def fetch_url(url: str, timeout: float, ua: str, body_cap: int = 5_000_000):
    """
    Return a dict with: final_url, status, content_type, body (bytes or None).
    - Follows redirects (via default handlers).
    - Reads up to body_cap bytes; None when non-HTML.
    """
    opener = build_opener(HTTPRedirectHandler, HTTPHandler, HTTPSHandler)
    req = Request(url, headers={"User-Agent": ua})
    try:
        with opener.open(req, timeout=timeout) as resp:
            final_url = resp.geturl()
            ctype = resp.headers.get("Content-Type", "") or ""
            status = getattr(resp, "status", 200)
            body = None
            if "text/html" in ctype.lower():
                body = resp.read(body_cap + 1)
                if len(body) > body_cap:
                    body = body[:body_cap]
            return {"final_url": final_url, "status": status, "content_type": ctype, "body": body}
    except HTTPError as e:
        return {"final_url": getattr(e, "url", url), "status": e.code, "content_type": "", "body": None}
    except URLError as e:
        raise
