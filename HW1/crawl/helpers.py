from urllib.parse import urlparse

def get_domain(url: str) -> str:
    """
    Extract registrable domain (eTLD+1).
    Simplified: last two labels, unless known multi-part TLDs (.co.uk, .ac.uk).
    """
    host = urlparse(url).hostname or ""
    parts = host.split(".")
    if len(parts) < 2:
        return host  # e.g. "localhost" or ""
    # handle common multi-part TLDs
    if parts[-2] in ("co", "ac") and parts[-1] == "uk":
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])

def get_superdomain(url: str) -> str:
    """
    Extract superdomain as the TLD bucket.
    """
    host = urlparse(url).hostname or ""
    parts = host.split(".")
    return parts[-1] if parts else ""
