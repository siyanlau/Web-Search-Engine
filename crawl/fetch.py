
from urllib.request import Request, build_opener, HTTPRedirectHandler, HTTPSHandler, HTTPHandler
from urllib.error import URLError, HTTPError

def fetch_url(url: str, timeout: float, ua: str):
    """
    Return a dict with: final_url, status, content_type, body (bytes or None).
    - Follows redirects (via default handlers).
    - Reads body only for text/html
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
                body = resp.read()
            return {"final_url": final_url, "status": status, "content_type": ctype, "body": body}
    except HTTPError as e:
        return {"final_url": getattr(e, "url", url), "status": e.code, "content_type": "", "body": None}
    except URLError as e:
        # Represent network issues as a string status
        reason = getattr(e, "reason", None)
        if reason:
            s = str(reason).lower()
            if "timed out" in s or "timeout" in s:
                status = "error:timeout"
            elif "name or service not known" in s or "nodename nor servname" in s:
                status = "error:dns"
            elif "ssl" in s:
                status = "error:ssl"
            else:
                status = "error:urlerror"
        else:
            status = "error:urlerror"
        return {"final_url": url, "status": status, "content_type": "", "body": None}
    except Exception as e:
        return {"final_url": url, "status": "error:" + e.__class__.__name__.lower(), "content_type": "", "body": None}
