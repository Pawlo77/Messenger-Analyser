import time
import logging

from setup import parse
from cleaning import clean


if __name__ == "__main__":
    t0 = time.time()

    t1 = time.time()
    parse()
    logging.info(f"Parsed args, took {time.time() - t1:.2f} seconds")

    t1 = time.time()
    clean()
    logging.info(f"Cleaned messages, took {time.time() - t1:.2f} seconds")

    logging.info(f"All tasks finished, took {time.time() - t0:.2f} seconds")
