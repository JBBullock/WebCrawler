import os
import shelve
import time

from collections import defaultdict, deque
from threading import RLock, Condition
from urllib.parse import urlparse

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid


class Frontier:
    """

    Thread-safe crawl frontier with per-domain politeness scheduling.

    Architecture
    ============
    Instead of one flat list, URLs are bucketed into per-domain deques:

        _domain_queues: { "www.ics.uci.edu": deque([url1, url2, ...]), ... }

    get_tbd_url() walks those buckets and returns the next URL whose domain
    has cooled down past the configured politeness delay.  If every domain is
    still in its cooldown window the call blocks (releases the GIL) until the
    nearest window expires, then retries.  It returns None only when every
    bucket is empty — i.e. the crawl is truly finished.

    Thread safety
    =============
    A single RLock (_lock) guards all mutable state (save, _domain_queues,
    _domain_last_fetch).  A Condition (_url_added) built on top of that lock
    lets add_url() wake sleeping get_tbd_url() callers the moment new work
    arrives, so threads never spin-wait.

    Politeness
    ==========
    _domain_last_fetch[domain] records the monotonic time at which a URL from
    that domain was last handed out.  get_tbd_url() will not return another
    URL from the same domain until  now >= last + config.time_delay.
    """

    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config

        # ── Per-domain URL queues (FIFO within each domain) ──────────────
        self._domain_queues: dict[str, deque] = defaultdict(deque)
        # ── Monotonic timestamps: when was a URL last handed out per domain
        self._domain_last_fetch: dict[str, float] = {}
        # ── Single lock covering all mutable state ───────────────────────
        self._lock = RLock()
        # ── Condition lets add_url() wake blocked get_tbd_url() callers ──
        self._url_added = Condition(self._lock)

        # ── Persistent save file ─────────────────────────────────────────
        # shelve appends platform suffixes (.db / .dir / .bak) on macOS,
        # so os.path.exists(save_file) alone is unreliable.
        def _save_exists(path: str) -> bool:
            return any(
                os.path.exists(path + suffix)
                for suffix in ("", ".db", ".dir", ".bak", ".dat")
            )

        if not _save_exists(self.config.save_file) and not restart:
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif _save_exists(self.config.save_file) and restart:
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            for suffix in ("", ".db", ".dir", ".bak", ".dat"):
                path = self.config.save_file + suffix
                if os.path.exists(path):
                    os.remove(path)

        self.save = shelve.open(self.config.save_file)



        if restart:
            for url in self.config.seed_urls:
                self.add_url(url, force =True)  # Use force for seeds
        else:
            self._parse_save_file()
            if not self._domain_queues:
                self.logger.info("No pending urls found, seeding from config.")
                for url in self.config.seed_urls:
                    self.add_url(url, force=True)  # Use force for seeds

        # CHANGE THIS LINE to include force=False
    def add_url(self, url: str, force: bool = False) -> None:
        """
        Normalise, deduplicate, persist, and enqueue a URL.
        """
        url = normalize(url)
        urlhash = get_urlhash(url)

        with self._url_added:
            # UPDATE THIS CHECK:
            # If it's in save, only return if we are NOT forcing it.
            if urlhash in self.save and not force:
                return

            # If we are forcing (seeding), we reset the 'completed' status to False
            self.save[urlhash] = (url, False)
            self.save.sync()

            domain = self._domain_of(url)
            self._domain_queues[domain].append(url)
            self._url_added.notify_all()
    # ──────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def _domain_of(url: str) -> str:
        """Return the netloc (host[:port]) of a URL."""
        return urlparse(url).netloc.lower()

    def _parse_save_file(self) -> None:
        """Reload unfinished URLs from a previous run into the domain queues."""
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                self._domain_queues[self._domain_of(url)].append(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from "
            f"{total_count} total urls discovered.")

    def _total_pending(self) -> int:
        """Total URLs across all domain queues. Must be called under _lock."""
        return sum(len(q) for q in self._domain_queues.values())

    # ──────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────

    def get_tbd_url(self) -> str | None:
        """
        Return the next URL the caller may fetch right now.

        Behaviour
        ---------
        - Prefers domains whose cooldown has already expired.
        - If every domain with pending URLs is still cooling down, blocks
          (releases the GIL via Condition.wait) until the earliest cooldown
          expires, then retries.  This prevents busy-spinning while still
          keeping all worker threads active on different domains.
        - Returns None only when every domain queue is empty.

        Complexity: O(D) per call where D = number of active domains (≤ 4
        in practice because MAX_URLS_PER_DOMAIN keeps D bounded).
        """
        with self._url_added:                       # acquires _lock
            while True:
                now = time.monotonic()
                earliest_ready = float("inf")       # soonest a domain unfreezes
                has_any_url = False

                for domain, queue in list(self._domain_queues.items()):
                    if not queue:
                        continue
                    has_any_url = True
                    last = self._domain_last_fetch.get(domain, 0.0)
                    wait_needed = (last + self.config.time_delay) - now

                    if wait_needed <= 0:
                        # ── Domain is ready ──────────────────────────────
                        url = queue.popleft()
                        if not queue:
                            del self._domain_queues[domain]
                        self._domain_last_fetch[domain] = time.monotonic()
                        return url

                    earliest_ready = min(earliest_ready, wait_needed)

                # ── Truly empty: crawl is done ───────────────────────────
                if not has_any_url:
                    return None

                # ── All domains cooling: sleep until the nearest window ──
                # Condition.wait(timeout) releases the lock so other threads
                # can call add_url() while we sleep.
                self._url_added.wait(timeout=earliest_ready)


    def mark_url_complete(self, url: str) -> None:
        """Mark a URL as finished in the persistent store."""
        urlhash = get_urlhash(url)
        with self._lock:
            if urlhash not in self.save:
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")
                return
            self.save[urlhash] = (url, True)
            self.save.sync()