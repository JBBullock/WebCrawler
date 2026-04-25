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
    Optimized to O(n) total complexity.
    """

    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config

        # -- Per-domain URL queues (FIFO within each domain) --------------
        self._domain_queues: dict[str, deque] = defaultdict(deque)
        # -- Monotonic timestamps: when was a URL last handed out per domain
        self._domain_last_fetch: dict[str, float] = {}

        # O(1) Optimization: Track the order of active domains.
        # This removes the O(D) scan from get_tbd_url.
        self._domain_order: deque = deque()

        # -- Single lock covering all mutable state -----------------------
        self._lock = RLock()
        # -- Condition lets add_url() wake blocked get_tbd_url() callers --
        self._url_added = Condition(self._lock)

        # -- Persistent save file -----------------------------------------
        if not os.path.exists(self.config.save_file) and not restart:
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)

        self.save = shelve.open(self.config.save_file)

        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    # ----------------------------------------------------------------------
    # Internal helpers
    # ----------------------------------------------------------------------

    @staticmethod
    def _domain_of(url: str) -> str:
        """Return the netloc (host[:port]) of a URL."""
        return urlparse(url).netloc.lower()

    def _parse_save_file(self) -> None:
        """Reload unfinished URLs O(n)."""
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                domain = self._domain_of(url)
                if not self._domain_queues[domain]:
                    self._domain_order.append(domain)
                self._domain_queues[domain].append(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from "
            f"{total_count} total urls discovered.")

    def _total_pending(self) -> int:
        """Total URLs across all domain queues. O(D)."""
        return sum(len(q) for q in self._domain_queues.values())

    # ----------------------------------------------------------------------
    # Public API
    # ----------------------------------------------------------------------

    def get_tbd_url(self) -> str | None:
        """
        Return the next URL ready for fetch in O(1) amortized time.
        """
        with self._url_added:  # acquires _lock
            while True:
                if not self._domain_order:
                    return None

                now = time.monotonic()
                # Check the domain that has been idle the longest
                domain = self._domain_order[0]
                queue = self._domain_queues[domain]

                if not queue:
                    self._domain_order.popleft()
                    if domain in self._domain_queues:
                        del self._domain_queues[domain]
                    continue

                last = self._domain_last_fetch.get(domain, 0.0)
                wait_needed = (last + self.config.time_delay) - now

                if wait_needed <= 0:
                    # -- Domain is ready ------------------------------
                    url = queue.popleft()
                    # Rotate the domain to the end for Round-Robin fairness
                    self._domain_order.rotate(-1)
                    self._domain_last_fetch[domain] = time.monotonic()
                    return url

                # All active domains are cooling. Since config.time_delay is
                # constant, the domain at the front will be the first to wake.
                self._url_added.wait(timeout=wait_needed)

    def add_url(self, url: str) -> None:
        """
        Normalise, deduplicate, persist, and enqueue a URL in O(1).
        """
        url = normalize(url)
        urlhash = get_urlhash(url)

        with self._url_added:  # acquires _lock
            if urlhash in self.save:
                return  # already seen

            self.save[urlhash] = (url, False)
            self.save.sync()  # flush to disk

            domain = self._domain_of(url)
            if not self._domain_queues[domain]:
                # New domain: ready immediately, place at the front
                self._domain_order.appendleft(domain)

            self._domain_queues[domain].append(url)
            self._url_added.notify_all()  # wake sleeping get_tbd_url

    def mark_url_complete(self, url: str) -> None:
        """Mark a URL as finished in the persistent store. O(1)."""
        urlhash = get_urlhash(url)
        with self._lock:
            if urlhash not in self.save:
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")
                return
            self.save[urlhash] = (url, True)
            self.save.sync()