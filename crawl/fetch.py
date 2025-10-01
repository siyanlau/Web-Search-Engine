
import urllib.request, urllib.error
import gzip, io, zlib

def fetch_url(url, timeout, ua):
    """
    Fetch a URL and return dict with {final_url, status, body}.
    - Always sets User-Agent and Accept-Encoding.
    - Explicitly handles gzip/deflate content-encoding.
    - Returns None body if not HTML.
    """
    headers = {
        "User-Agent": ua,
        # ask for compressed data, since we know how to handle it
        "Accept-Encoding": "gzip, deflate",
    }
    req = urllib.request.Request(url, headers=headers)

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            final_url = resp.geturl()
            status = resp.status
            raw = resp.read()

            # decompress if needed
            encoding = resp.headers.get("Content-Encoding", "").lower()
            if encoding == "gzip":
                try:
                    raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
                except Exception:
                    pass  # fall back to raw if decompression fails
            elif encoding == "deflate":
                try:
                    raw = zlib.decompress(raw, -zlib.MAX_WBITS)
                except Exception:
                    try:
                        raw = zlib.decompress(raw)
                    except Exception:
                        pass

            # content-type check
            ctype = resp.headers.get("Content-Type", "").lower()
            if "text/html" not in ctype:
                body = None
            else:
                body = raw

            return {"final_url": final_url, "status": status, "body": body}

    except urllib.error.HTTPError as e:
        return {"final_url": url, "status": e.code, "body": None}
    except Exception as e:
        return {"final_url": url, "status": f"error:{type(e).__name__}", "body": None}