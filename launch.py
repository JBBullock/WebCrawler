import multiprocessing
multiprocessing.set_start_method("fork")

import argparse

from configparser import ConfigParser
from argparse import ArgumentParser

from utils.server_registration import get_cache_server
from utils.config import Config
from crawler import Crawler


def main(config_file, restart):
    cparser = ConfigParser()
    cparser.read(config_file)
    config = Config(cparser)
    config.cache_server = get_cache_server(config, restart)
    crawler = Crawler(config, restart)
    crawler.start()


if __name__ == "__main__":
    import shelve

    # Path to the save file defined in your config
    save_file_path = "frontier.shelve"

    with shelve.open(save_file_path) as db:
        print(f"{'URL':<60} | {'Completed'}")
        print("-" * 75)
        for urlhash in db:
            url, completed = db[urlhash]
            if completed:
                print(f"{url:<60} | {completed}")

    """Above was used to print out the contents of the frontier shelve database, 
    below is the code that was used to launch the webcrawler."""
    # parser = ArgumentParser()
    # parser.add_argument("--restart", action="store_true", default=False)
    # parser.add_argument("--config_file", type=str, default="config.ini")
    # args = parser.parse_args()
    # main(args.config_file, args.restart)
