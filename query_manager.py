import os
import json

from typing import List

from setup import Config
from queries import CountMessagesQuery, MessagesOnTime, Top10withoutGroups, MostCommonStrings



QUERIES = (CountMessagesQuery(), MessagesOnTime(), Top10withoutGroups(), MostCommonStrings())


class QueryExecutor:
    def __init__(self, data_file_path: str = None) -> None:
        if data_file_path is None:
            config_path = Config.get("output_dir_path")
            if config_path.startswith("/"):
                data_file_path = config_path
            else:
                data_file_path = os.path.join(
                    os.path.dirname(__file__), Config.get("output_dir_path")
                )
            data_file_path = os.path.join(
                data_file_path,
                Config.get("prefix") + "_" + "conversations.json",
            )

        with open(data_file_path, "r", encoding="utf-8") as file:
            self.data = json.load(file)

        # check if it is exacly data format we expect, ie produced by our cleaner:
        assert isinstance(
            self.data, list
        ), "Wrong data format (check expected format produced by CleaningExecutor)"
        for l in self.data:
            assert (
                len(l) >= 3 and isinstance(l[-2], list) and isinstance(l[-1], int)
            ), "Wrong data format (check expected format produced by CleaningExecutor)"

    def __call__(self, *args: List[int], **kwargs) -> None:
        if len(args) == 0:
            args = Config.get("queries")

            if len(args) == 0:  # user provided no queries - take all
                args = list(range(len(QUERIES)))

        for q in args:
            assert 0 <= int(q) < len(QUERIES), f"Wrong query id provided (got {q})."
            QUERIES[int(q)](self.data, **kwargs)


def query(data_file_path: str = None, *args: List[int], **kwargs):
    executor = QueryExecutor(data_file_path)
    executor(*args, **kwargs)
