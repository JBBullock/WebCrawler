
"""scraper.py  –
UCI Web Crawler  (redesigned by Claude for correctness and optimization)
"""

import hashlib
import re
from collections import Counter, defaultdict
from urllib.parse import urljoin, urldefrag, urlparse, parse_qs


# ============================================================
# Configuration – tweak these without touching logic below
# ============================================================

VALID_DOMAINS = (
    ".ics.uci.edu",
    ".cs.uci.edu",
    ".informatics.uci.edu",
    ".stat.uci.edu",
)

MAX_CONTENT_BYTES   = 5 * 1024 * 1024   # 5 MB hard cap before parsing
MIN_WORD_COUNT      = 50               # pages with fewer words are low-value
MIN_TEXT_RATIO      = 0.02             # text chars / total HTML chars
MAX_URL_DEPTH       = 6                 # path segments (/ separated)
MAX_QUERY_PARAMS    = 4                 # too many params -> likely a trap
MAX_URLS_PER_DOMAIN = 500              # per-domain crawl budget
SIMHASH_BITS        = 64
SIMHASH_MAX_DIST    = 3                 # Hamming distance threshold


# ============================================================
# Global state  (shared across all scraper() calls)
# ============================================================

# Fingerprinting
_exact_fingerprints: set = set()   # sha256 hex strings
# Optimized SimHash storage: maps (band_index, band_value) -> set of full hashes
_simhash_store: defaultdict = defaultdict(set)

# Trap / budget guards
_domain_url_count: defaultdict = defaultdict(int)   # domain -> # URLs seen

# Analytics
page_word_counts:  dict        = {}              # url -> word count
word_freq_global:  Counter     = Counter()       # aggregate word frequencies
subdomains:        defaultdict = defaultdict(int) # netloc -> unique-page count


# ============================================================
# Public entry point
# ============================================================

def scraper(url: str, resp) -> list:
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]


# ============================================================
# Link extraction
# ============================================================

def extract_next_links(url: str, resp) -> list:
    if resp.status != 200 or resp.raw_response is None:
        return []

    raw = resp.raw_response
    content = raw.content or b""
    if not content:
        return []

    if len(content) > MAX_CONTENT_BYTES:
        _log(f"SKIP (too large {len(content)//1024} KB): {url}")
        return []

    headers = getattr(raw, "headers", {}) or {}
    content_type = headers.get("Content-Type", "") or headers.get("content-type", "")
    if content_type and "text/html" not in content_type:
        _log(f"SKIP (non-HTML {content_type!r}): {url}")
        return []

    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(content, "lxml")
    except Exception as exc:
        _log(f"PARSE ERROR ({exc}): {url}")
        return []

    raw_html_len = len(content)
    text = soup.get_text(separator=" ", strip=True)
    text_len = len(text)

    if text_len == 0:
        _log(f"SKIP (dead/empty text): {url}")
        return []

    text_ratio = text_len / max(raw_html_len, 1)
    if text_ratio < MIN_TEXT_RATIO:
        _log(f"SKIP (low text ratio {text_ratio:.2%}): {url}")
        return []

    words = re.findall(r"[a-zA-Z]{2,}", text.lower())
    if len(words) < MIN_WORD_COUNT:
        _log(f"SKIP (only {len(words)} words): {url}")
        return []

    fp = _sha256(text)
    if fp in _exact_fingerprints:
        _log(f"SKIP (exact dup): {url}")
        return []
    _exact_fingerprints.add(fp)

    sh = _simhash(words)
    if _is_near_duplicate(sh):
        _log(f"SKIP (near dup, simhash {sh:016x}): {url}")
        return []

    # Store SimHash in 4 bands of 16 bits for O(1) Hamming lookup
    for i in range(4):
        band = (sh >> (i * 16)) & 0xFFFF
        _simhash_store[(i, band)].add(sh)

    page_word_counts[url] = len(words)
    word_freq_global.update(words)
    subdomains[urlparse(url).netloc] += 1

    extracted = []
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if not href or href.startswith(("javascript:", "mailto:", "tel:")):
            continue
        absolute, _ = urldefrag(urljoin(url, href))
        extracted.append(absolute)

    return extracted


# ============================================================
# URL validation
# ============================================================

_TRAP_QUERY_RE = re.compile(
    r"\b(share|print|replytocom|attachment|download|export|format|"
    r"version|lang|locale|currency|sort|order|filter|tab|view|"
    r"session|token|sid|csrf|nonce|redirect|return|ref|source|"
    r"utm_\w+|fb_\w+|gclid|fbclid)\b",
    re.IGNORECASE,
)

_CALENDAR_PATH_RE = re.compile(
    r"/\d{4}/\d{1,2}(/\d{1,2})?/"
    r"|/\d{4}-\d{2}(-\d{2})?"
    r"|/(january|february|march|april|may|june|july|august|"
    r"september|october|november|december)/",
    re.IGNORECASE,
)


def _has_repeated_path_segment(path: str) -> bool:
    parts = [p for p in path.split("/") if p]
    if not parts: return False
    counts = Counter(parts)
    return any(v >= 3 for v in counts.values())


def is_valid(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False

    if parsed.scheme not in {"http", "https"}:
        return False

    hostname = parsed.netloc.lower()
    if not any(hostname == d.lstrip(".") or hostname.endswith(d) for d in VALID_DOMAINS):
        return False

    domain_key = parsed.netloc.lower()
    if _domain_url_count[domain_key] >= MAX_URLS_PER_DOMAIN:
        return False
    _domain_url_count[domain_key] += 1

    if re.search(
        r"\.(css|js|bmp|gif|jpe?g|ico|png|tiff?|svg|webp"
        r"|mid|mp2|mp3|mp4|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
        r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
        r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
        r"|epub|dll|cnf|tgz|sha1|thmx|mso|arff|rtf|jar|csv"
        r"|rm|smil|wmv|swf|wma|zip|rar|gz|json|xml|rss|atom)$",
        parsed.path.lower(),
    ):
        return False

    depth = parsed.path.count("/")
    if depth > MAX_URL_DEPTH:
        return False

    if len(parse_qs(parsed.query)) > MAX_QUERY_PARAMS:
        return False

    if _TRAP_QUERY_RE.search(parsed.query) or _CALENDAR_PATH_RE.search(parsed.path):
        return False

    return not _has_repeated_path_segment(parsed.path)


# ============================================================
# SimHash (Optimized)
# ============================================================

def _simhash(tokens: list) -> int:
    freq = Counter(tokens)
    v = [0] * SIMHASH_BITS
    for token, weight in freq.items():
        # MD5 is O(len(token)), total is O(n)
        h = int(hashlib.md5(token.encode()).hexdigest(), 16) & ((1 << SIMHASH_BITS) - 1)
        for i in range(SIMHASH_BITS):
            v[i] += weight if (h >> i) & 1 else -weight

    fingerprint = 0
    for i in range(SIMHASH_BITS):
        if v[i] > 0:
            fingerprint |= 1 << i
    return fingerprint


def _is_near_duplicate(sh: int) -> bool:
    """O(1) average case lookup using Multi-Index Hashing."""
    for i in range(4):
        band = (sh >> (i * 16)) & 0xFFFF
        if (i, band) in _simhash_store:
            for candidate in _simhash_store[(i, band)]:
                # Hamming distance via bit_count (optimized in Python 3.10+)
                if (sh ^ candidate).bit_count() <= SIMHASH_MAX_DIST:
                    return True
    return False


# ============================================================
# Utility & Reporting
# ============================================================

def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()

def _log(msg: str) -> None:
    print(f"[scraper] {msg}")

STOP_WORDS = {
    "the", "of", "and", "to", "a", "in", "is", "it", "you", "that", "he",
    "was", "for", "on", "are", "with", "as", "at", "this", "be", "by",
    "from", "or", "an", "but", "not", "have", "had", "they", "which",
    "one", "all", "were", "we", "her", "she", "do", "his", "if", "will",
    "up", "more", "no", "out", "so", "said", "what", "its", "about",
    "than", "into", "them", "can", "only", "other", "new", "some", "time",
    "could", "these", "two", "may", "then", "first", "any", "my",
    "now", "such", "like", "our", "over", "also", "back", "after", "use",
    "how", "their", "has", "your", "each", "just",
}

def report_top_words(n: int = 50) -> list:
    filtered = {w: c for w, c in word_freq_global.items() if w not in STOP_WORDS}
    return sorted(filtered.items(), key=lambda x: x[1], reverse=True)[:n]

def report_longest_page() -> tuple:
    if not page_word_counts: return ("", 0)
    url = max(page_word_counts, key=page_word_counts.get)
    return url, page_word_counts[url]

def report_subdomains() -> list:
    return sorted(subdomains.items())