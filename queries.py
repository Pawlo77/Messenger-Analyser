import pandas as pd

from typing import Any, List

from helpers import Query


class CountMessagesQuery(Query):
    def __init__(
        self,
        min_messages_num: List[int] = [0, 3, 7, 15],
    ) -> None:
        super().__init__("count_messages", ".csv")
        self.min_messages_num = min_messages_num

    def execute(self, data: List[tuple], **kwargs) -> Any:
        dates = {}

        for line in data:
            message, timestamp = line[-2], line[-1]
            date = self.get_date(timestamp)

            if date not in dates:
                dates[date] = {str(key): 0 for key in self.min_messages_num}

            for min_message_num in self.min_messages_num:
                if len(message) > min_message_num:
                    dates[date][str(min_message_num)] += 1

        result = pd.DataFrame(dates)
        result = result.T
        columns = ["date"] + [f"min_messsages={num}" for num in result.columns]
        result = result.reset_index()
        result.columns = columns
        return result
