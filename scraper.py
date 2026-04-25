import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
from collections import defaultdict

"""This is an edit to have a commit to come back, check git commit for more details"""

# --- Analytics storage (in-memory, persist however you like) ---
page_word_counts = {}          # url -> word count
subdomains = defaultdict(int)  # subdomain -> number of unique pages

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]


def extract_next_links(url, resp):
    """
    Parse the response and return a list of defragmented absolute URLs
    found on the page. Also collects analytics (word count, subdomains).

    Args:
        url  : the URL used to fetch the page
        resp : response object with .status, .error, .raw_response

    Returns:
        list[str]: absolute, defragmented hyperlinks scraped from the page
    """
    # Only process successful responses
    if resp.status != 200 or resp.raw_response is None:
        return []

    content = resp.raw_response.content

    # Guard against empty or non-HTML content
    if not content:
        return []

    try:
        soup = BeautifulSoup(content, "lxml")
    except Exception:
        return []

    # --- Analytics: word count for this page ---
    text = soup.get_text(separator=" ")
    words = re.findall(r"[a-zA-Z0-9']+", text.lower())
    page_word_counts[url] = len(words)

    # --- Analytics: track unique pages per subdomain ---
    parsed_url = urlparse(url)
    subdomains[parsed_url.netloc] += 1

    # --- Extract all <a href="..."> links ---
    extracted = []
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if not href:
            continue

        # Resolve relative URLs against the base page URL
        absolute = urljoin(url, href)

        # Remove fragment (the #section part)
        defragmented, _ = urldefrag(absolute)

        extracted.append(defragmented)

    return extracted


# ---------------------------------------------------------------------------
# Valid domains for this UCI crawler assignment
# ---------------------------------------------------------------------------
VALID_DOMAINS = (
    ".ics.uci.edu",
    ".cs.uci.edu",
    ".informatics.uci.edu",
    ".stat.uci.edu",
)

# Paths that are known crawler traps or produce near-infinite URL spaces
TRAP_PATTERNS = re.compile(
    r"(calendar|date|event|filter|sort|page|session|sid|token"
    r"|login|logout|signup|register|download|share|print|feed"
    r"|replytocom|attachment|wp-login|wp-admin"
    r"|utm_|ref=|source=|version=|lang=|do=|action=|redirect)",
    re.IGNORECASE,
)


def is_valid(url):
    """
    Return True if the crawler should visit this URL, False otherwise.
    Criteria:
      - http or https scheme
      - within the allowed UCI domains
      - not a static asset (image, pdf, archive, etc.)
      - not an obvious crawler trap
    """
    try:
        parsed = urlparse(url)

        # Must be http(s)
        if parsed.scheme not in {"http", "https"}:
            return False

        # Must belong to one of the allowed UCI domains
        hostname = parsed.netloc.lower()
        if not any(hostname == d.lstrip(".") or hostname.endswith(d)
                   for d in VALID_DOMAINS):
            return False

        # Skip URLs that look like crawler traps (long query strings, etc.)
        if TRAP_PATTERNS.search(parsed.query):
            return False

        # Skip extremely long URLs (common sign of a trap)
        if len(url) > 300:
            return False

        # Skip static/binary file extensions
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            r"|png|tiff?|mid|mp2|mp3|mp4"
            r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            r"|epub|dll|cnf|tgz|sha1"
            r"|thmx|mso|arff|rtf|jar|csv"
            r"|rm|smil|wmv|swf|wma|zip|rar|gz)$",
            parsed.path.lower(),
        )

    except TypeError:
        print("TypeError for", parsed)
        raise


# ---------------------------------------------------------------------------
# Report helpers (call these after the crawl to answer assignment questions)
# ---------------------------------------------------------------------------

def get_most_common_words(stop_words=None, top_n=50):
    """
    Aggregate word frequencies across all crawled pages.
    Pass in a set of stop-words to exclude common English words.
    """
    if stop_words is None:
        stop_words = set()

    freq = defaultdict(int)
    for counts in _word_freq_per_page.values():
        for word, count in counts.items():
            if word not in stop_words:
                freq[word] += count

    return sorted(freq.items(), key=lambda x: x[1], reverse=True)[:top_n]


def get_page_with_most_words():
    """Return (url, word_count) for the page with the highest word count."""
    if not page_word_counts:
        return None, 0
    url = max(page_word_counts, key=page_word_counts.get)
    return url, page_word_counts[url]


def get_subdomains():
    """Return subdomain -> unique page count, sorted alphabetically."""
    return dict(sorted(subdomains.items()))