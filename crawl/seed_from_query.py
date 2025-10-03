import requests
from bs4 import BeautifulSoup

def get_seeds_from_query(query, num_results=10):
    """
    Fetch search results from DuckDuckGo and return a list of URLs.
    """
    url = "https://duckduckgo.com/html/"
    params = {"q": query}
    headers = {"User-Agent": "Mozilla/5.0"}

    r = requests.get(url, params=params, headers=headers, timeout=10)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    results = []
    for a in soup.select("a.result__a")[:num_results]:
        results.append(a["href"])
    return results
