import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, unquote

def get_seeds_from_query(query, num_results=10):
    url = "https://duckduckgo.com/html/"
    params = {"q": query}
    headers = {"User-Agent": "Mozilla/5.0"}

    r = requests.get(url, params=params, headers=headers, timeout=10)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    results = []
    for a in soup.select("a.result__a")[:num_results]:
        href = a["href"]
        # Handle DDG redirect wrapper
        if "uddg=" in href:
            qs = parse_qs(urlparse(href).query)
            if "uddg" in qs:
                real_url = unquote(qs["uddg"][0])
                results.append(real_url)
            else:
                results.append("https:" + href)  # fallback
        else:
            results.append(href)

    return results
