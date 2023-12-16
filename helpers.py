import os
import json
import logging
import pandas as pd

from typing import Any, List
from itertools import count
from datetime import datetime

from setup import Config


class Query:
    def __init__(
        self,
        id: str,
        result_extension: str = ".csv",
    ) -> None:
        self.id = id
        self.path = f"{Config.get('prefix')}_query_{id}{result_extension}"

    def __call__(self, data: List[tuple], **kwargs):
        logging.info(f"Query_{self.id}:Execution started.")
        result = self.execute(data, **kwargs)
        Query.save(result, self.path)
        logging.info(
            f"Query_{self.id}:Execution finished, results saved to {self.path}."
        )

    def execute(self, data: List[tuple], **kwargs) -> Any:
        return data

    def get_date(self, timestamp: int):
        timestamp = timestamp / 1000
        return datetime.utcfromtimestamp(timestamp).replace(second= 0, microsecond= 0)

    @staticmethod
    def save(result: Any, path: str) -> None:
        if not os.path.isabs(path):
            path = os.path.join(Config.get("output_dir_path"), path)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        if os.path.exists(path):
            logging.info(f"Overwritting {path}")

        _, extension = os.path.splitext(path)

           
        if not isinstance(result, pd.DataFrame):
            result = pd.DataFrame(result)
        result.to_csv(path, index=None)
            

    @staticmethod
    def get_groups(data: List[tuple]):
        conversation_users = {}
        for conversation_id, user_id, _, timestamp in data:
            if conversation_id in conversation_users.keys():
                conversation_users[conversation_id].add(user_id)
            else:
                conversation_users[conversation_id] = set([user_id])
        return set(key for key, value in conversation_users.items() if len(value) > 2)
    
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
