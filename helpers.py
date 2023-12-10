import os
import pandas as pd

from typing import Any
from itertools import count

from setup import Config


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
