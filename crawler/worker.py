from threading import Thread
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time

MAX_WORKERS = 4  # upper bound on concurrent download threads


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier

        # O(1) Optimization: Fetch source once instead of repeated calls in comprehensions
        scraper_src = getsource(scraper)
        forbidden_reqs = {
            "from requests import", "import requests",
            "from urllib.request import", "import urllib.request"
        }

        # Basic check for forbidden requests in scraper
        assert all(scraper_src.find(req) == -1 for req in forbidden_reqs), \
            "Do not use requests or urllib.request in official_scraper.txt"

        super().__init__(daemon=True)

    # ------------------------------------------------------------------
    # Per-URL unit of work (runs inside a pool thread)
    # ------------------------------------------------------------------

    def _process_url(self, url: str) -> None:
        """
        Download one URL, scrape it, and enqueue the discovered links.

        Complexity: O(L) where L = number of links found on the page.
        Summed over all n pages → O(n) total link-enqueue work.
        """
        resp = download(url, self.config, self.logger)
        self.logger.info(
            f"Downloaded {url}, status <{resp.status}>, "
            f"using cache {self.config.cache_server}."
        )

        # Direct iteration ensures O(L) processing per page
        for scraped_url in scraper.scraper(url, resp):
            self.frontier.add_url(scraped_url)

        self.frontier.mark_url_complete(url)
        time.sleep(self.config.time_delay)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """
        Drive up to MAX_WORKERS concurrent downloads via a thread pool.

        Complexity analysis → O(n)
        ──────────────────────────────────────────────────────────────
        Each URL is handled as a discrete unit. Since MAX_WORKERS is a
        constant (4), the 'pending' set management and wait() calls
        scale linearly with the number of URLs (n).
        """
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            pending: set = set()

            while True:
                # ── Fill the pool up to capacity ──────────────────────────
                # O(MAX_WORKERS) = O(1) per cycle.
                while len(pending) < MAX_WORKERS:
                    tbd_url = self.frontier.get_tbd_url()
                    if tbd_url is None:
                        break
                    pending.add(executor.submit(self._process_url, tbd_url))

                # ── Termination: nothing running and nothing to fetch ──────
                if not pending:
                    self.logger.info("Frontier is empty. Stopping Crawler.")
                    break

                # ── Block until at least one task finishes ─────────────────
                # O(1) because |pending| is capped at a constant.
                done, pending = wait(pending, return_when=FIRST_COMPLETED)

                # ── Surface worker-thread errors immediately ───────────────
                for future in done:
                    exc = future.exception()
                    if exc:
                        self.logger.error(f"Worker thread raised: {exc}")