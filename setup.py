import os
import warnings
import logging
import argparse

from typing import Any, List, Tuple


DESCRIPTION = """
Facebook data formatter. Drops photos, encodes all found users into unique ids,
performs messages lemmatization and links and emoji encodings. Can Tokenize message contents.
"""

ARGUMENTS = (
    (
        "input_dir_path",
        os.path.join(os.path.dirname(__file__), "data"),
        "Directory holding folders containing facebook data in json format.",
        False,
        str,
    ),
    (
        "output_dir_path",
        os.path.join(os.path.dirname(__file__), "output"),
        "Directory to which output should be written.",
        False,
        str,
    ),
    ("n_threds", 8, "Number of threads to be used for processing.", False, int),
    ("prefix", "0", "Prefix added to each identifier.", False, str),
    (
        "default_language",
        "pl",
        "If program will be unable to detect language it will use that.",
        False,
        str,
    ),
    (
        "verbose",
        0,
        "Verbosity mode - 0 for None, 1 for logging without warnings, 2 for all.",
        False,
        int,
    ),
)


class Config:
    class ConfigPlaceholder:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    config = ConfigPlaceholder()

    @staticmethod
    def assert_str(**kwargs):
        for k in kwargs.keys():
            assert isinstance(k, str), f"key ({k}): is not string."

    @staticmethod
    def map_args_to_kwargs(*args: List[Tuple[str, Any]], **kwargs):
        if len(args) > 0:
            for name, value in args:
                kwargs[name] = value
        return kwargs

    @staticmethod
    def get(name: str, not_found: Any = -float("inf")):
        Config.assert_str(name=name)
        return Config.config.__dict__.get(name, not_found)

    @staticmethod
    def set(*args: List[Tuple[str, Any]], **kwargs):
        kwargs = Config.map_args_to_kwargs(*args, **kwargs)
        Config.assert_str(**kwargs)
        Config.config.__dict__.update(kwargs)

    @staticmethod
    def update(*args: List[Tuple[str, Any]], **kwargs):
        kwargs = Config.map_args_to_kwargs(*args, **kwargs)
        Config.assert_str(**kwargs)
        for name, value in kwargs.items():
            if name in Config.config.__dict__.keys():
                Config.config.__dict__[name] = value
            else:
                warnings.warn(f"Key {name} is not present in config.")


def parse():
    parser = argparse.ArgumentParser(description=DESCRIPTION)

    for name, default, desc, req, type_ in ARGUMENTS:
        parser.add_argument(
            "--" + name, default=default, help=desc, required=req, type=type_
        )
    args = parser.parse_args()

    Config.set(*args._get_kwargs())
    if Config.get("verbose") > 0:
        logging.basicConfig(level=logging.DEBUG)

        if Config.get("verbose") == 1:
            warnings.simplefilter("ignore")
    else:
        logging.basicConfig(level=logging.ERROR)
