import os
import json
import logging
import warnings
import pandas as pd

from typing import Any, List
from itertools import count
from datetime import datetime

from setup import Config


def encode(*args: List[Any]) -> List[Any]:
    args = list(args)
    for i in range(len(args)):
        try:
            if isinstance(args[i], str):
                args[i] = args[i].encode("latin-1").decode("utf-8", "ignore")
        except Exception as e:
            warnings.warn(f"Could not encode: {args[i]} - {e}")

    return args


# returns (name, gender)
encode_user = (
    lambda user_id, users: users[user_id]
    if user_id in users.keys()
    else ("unknown", "unknown")
)
# returns (is_group, number_of_participants)
encode_group = (
    lambda conversation_id, groups: (
        len(groups[conversation_id]) > 2,
        len(groups[conversation_id]),
    )
    if conversation_id in groups.keys()
    else ("unknown", "unknown")
)


class Query:
    def __init__(
        self,
        id: str,
        result_extension: str = ".csv",
        timestamp_group_format: str = "%Y-%m-%d %H:00",
    ) -> None:
        self.id = id
        self.path = f"{Config.get('prefix')}_query_{id}{result_extension}"
        self.timestamp_group_format = timestamp_group_format

    def __call__(self, data: List[tuple], **kwargs):
        logging.info(f"Query_{self.id}:Execution started.")
        assert len(data) > 0, "Empty data list."
        result = self.execute(data, **kwargs)
        Query.save(result, self.path)
        logging.info(
            f"Query_{self.id}:Execution finished, results saved to {self.path}."
        )

    def execute(self, data: List[tuple], **kwargs) -> Any:
        return data

    def get_date(self, timestamp: int):
        return datetime.fromtimestamp(timestamp / 1000).strftime(
            self.timestamp_group_format
        )

    def get_from_kw(self, kw: dict, name: str, assert_val: Any = None) -> Any:
        val = kw.pop(name, None)
        assert val is not assert_val, f"Query: {self.id} requires {name} to work."
        return val

    @staticmethod
    def save(result: Any, path: str) -> None:
        if not os.path.isabs(path):
            path = os.path.join(Config.get("output_dir_path"), path)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        if os.path.exists(path):
            logging.info(f"Overwritting {path}")

        _, extension = os.path.splitext(path)

        if extension == ".csv":
            if not isinstance(result, pd.DataFrame):
                result = pd.DataFrame(result)
            result.to_csv(path, index=None)
        else:
            with open(path, "w") as file:
                return json.dump(result, file, ensure_ascii=False)

    @staticmethod
    def get_groups(data: List[tuple]) -> dict:
        conversation_users = {}
        for conversation_id, user_id, _, timestamp in data:
            if conversation_id in conversation_users.keys():
                conversation_users[conversation_id].add(user_id)
            else:
                conversation_users[conversation_id] = set([user_id])
        # return set(key for key, value in conversation_users.items() if len(value) > 2)
        return conversation_users

    @staticmethod
    def reverse_df(
        df: pd.DataFrame, by: str = "date", sort: bool = True
    ) -> pd.DataFrame:
        columns = df[by]
        df.drop(by, axis=1, inplace=True)
        df = df.T
        df = df.reset_index()
        df.columns = [by] + list(columns)

        if sort:
            return df.sort_values(by=by)
        return df


class GenderPredictorForPolishNames:
    def __init__(self):
        self.names = pd.read_excel(
            os.path.join(os.path.dirname(__file__), "resources", "imiona_polskie.xlsx")
        )
        self.names = {
            imie.lower(): plec for imie, plec in zip(self.names.imie, self.names.plec)
        }

    def predict_gender(self, name):
        gender = self.names.get(name, "unknown")
        if gender == "unknown" and name.endswith("a"):
            return "female"
        return gender


class Counter:
    COUNTER = count(start=1)

    def __init__(self, lock) -> None:
        self.counter = count(start=1)
        self.dt = {}
        self.lock = lock

    def set(self, key: str, value: Any) -> None:
        with self.lock:
            self.dt[key] = value

    def get(self, key: str) -> Any:
        return self.dt.get(key, None)

    def get_id(self) -> int:
        with self.lock:
            return f"{Config.get('prefix')}_{next(self.counter)}"
