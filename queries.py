import pandas as pd
import json
from typing import Any, List
from datetime import datetime
from helpers import Config
from helpers import Query


class CountMessagesQuery(Query):
    """
    Groups messages by conversation_id, date (rrrr-mm-dd) and returns
    data as conversation_id, data, is_group, number of messages that exceed length num for
    each entry defined in min_messages_num
    """

    def __init__(
        self,
        min_messages_num: List[int] = [0, 3, 7, 15],
    ) -> None:
        super().__init__("count_messages", ".csv")
        self.min_messages_num = min_messages_num

    def execute(self, data: List[tuple], **kwargs) -> Any:
        assert len(data) > 0

        groups = kwargs.pop("groups", None)
        assert groups is not None, f"Query: {self.id} requires groups to work."

        get_entry = lambda conversation_id, message: [conversation_id in groups] + [
            1 if len(message) >= num else 0 for num in self.min_messages_num
        ]

        counts = {}
        for conversation_id, _, message, timestamp in data:
            date = self.get_date(timestamp)

            if conversation_id in counts.keys():
                if date in counts[conversation_id].keys():
                    for i, num in enumerate(self.min_messages_num):
                        if len(message) >= num:
                            counts[conversation_id][date][i + 1] += 1
                else:
                    counts[conversation_id][date] = get_entry(conversation_id, message)

            else:
                counts[conversation_id] = {date: get_entry(conversation_id, message)}

        return pd.DataFrame(
            [
                (outer_key, inner_key, *inner_values)
                for outer_key, outer_values in counts.items()
                for inner_key, inner_values in outer_values.items()
            ],
            columns=["conversation_id", "date", "is_group"]
            + [f"min_messages={num}" for num in self.min_messages_num],
        )


class MessagesOnTime(Query):
    def __init__(self) -> None:
        super().__init__(id="message_on_time", result_extension=".csv")

    def execute(self, data: List[tuple], **kwargs) -> Any:
        df_dict = {}
        with open(
            Config.get("output_dir_path")
            + "/"
            + Config.get("prefix")
            + "_users_reversed.json",
            "r",
            encoding="utf-8",
        ) as file:
            data_dict = json.load(file)
        title0 = data[0][0]
        user_count = 0
        users = []
        pom = {}
        group = False
        for line in data:
            title, user_id, timestamp = line[-4], line[-3], line[-1]
            timestamp = timestamp / 1000
            date = datetime.utcfromtimestamp(timestamp)
            gender = data_dict.get(user_id)[1]
            if title != title0:
                if group:
                    pom = dict(
                        map(lambda kv: (kv[0], Query.add_group(kv[1], True))),
                        pom.items(),
                    )
                else:
                    pom = dict(
                        map(lambda kv: (kv[0], Query.add_group(kv[1], False))),
                        pom.items(),
                    )

                pom.update(df_dict)
                df_dict = pom
                pom = {}
                user_count = 0
                users = []
                title0 = title
                group = False

            if user_id not in users and user_count < 3:
                user_count += 1
                users.append(user_id)
            if title == title0 and user_count > 2:
                group = True

            key = (date, gender)
            if key not in pom:
                pom[key] = 1

        if group:
            pom = dict(
                map(
                    lambda kv: (kv[0], MessagesOnTime.update_valueT(kv[1])), pom.items()
                )
            )
        else:
            pom = dict(
                map(
                    lambda kv: (kv[0], MessagesOnTime.update_valueF(kv[1])), pom.items()
                )
            )
        pom.update(df_dict)
        df_dict = pom
        df = pd.DataFrame(
            [
                (
                    key[0].replace(second=0, microsecond=0),
                    key[1],
                    value["val"],
                    value["group"],
                )
                for key, value in df_dict.items()
            ],
            columns=["date", "gender", "count", "group"],
        )

        return df

    def update_valueT(value):
        return {"val": value, "group": True}

    @staticmethod
    def update_valueF(value):
        return {"val": value, "group": False}


class Top10withoutGroups(Query):
    def __init__(self) -> None:
        super().__init__(id="top10mess", result_extension=".csv")

    def execute(self, data: List[tuple], **kwargs) -> Any:
        df_dict = {}
        with open(
            Config.get("output_dir_path")
            + "/"
            + Config.get("prefix")
            + "_users_reversed.json",
            "r",
            encoding="utf-8",
        ) as file:
            data_dict = json.load(file)
        title0 = data[0][0]
        user_count = 0
        users = []
        pom = {}
        group = False
        for line in data:
            title, user_id = line[-4], line[-3]
            if title != title0:
                if not group:
                    pom.update(df_dict)
                    df_dict = pom
                pom = {}
                user_count = 0
                users = []
                title0 = title
                group = False
            if group:
                continue
            if user_id not in users and user_count < 3:
                user_count += 1
                users.append(user_id)
            if title == title0 and user_count > 2:
                group = True
            if user_id not in pom:
                pom[user_id] = {"val": 1, "user": data_dict.get(user_id)[0]}
            else:
                pom[user_id]["val"] += 1
        if not group:
            pom.update(df_dict)
            df_dict = pom
        df = pd.DataFrame(
            [(value["user"], value["val"]) for key, value in df_dict.items()],
            columns=["user", "count"],
        )
        df = df.sort_values(by="count", ascending=False).head(10)

        return df


class MostCommonStrings(Query):
    def __init__(self) -> None:
        super().__init__(id="mostcommonstrings", result_extension=".csv")

    def execute(self, data: List[tuple], **kwargs) -> Any:
        df_dict = {}
        with open(
            Config.get("output_dir_path")
            + "/"
            + Config.get("prefix")
            + "_users_reversed.json",
            "r",
            encoding="utf-8",
        ) as file:
            data_dict = json.load(file)
        for line in data:
            user_id, message = line[-3], line[-2]
            if user_id != Config.get("user_id"):
                continue
            for i in range(len(message) - 1):
                pair_user = message[i] + " " + message[i + 1], user_id
                if pair_user in df_dict:
                    df_dict[pair_user] += 1
                else:
                    df_dict[pair_user] = 1
        df = pd.DataFrame(
            [
                (data_dict.get(key[1])[0], key[0], value)
                for key, value in df_dict.items()
            ],
            columns=["user", "pair", "count"],
        )
        df = df.sort_values("count", ascending=False)
        return df


# najczestrze słowa idące w parze/trójce - pewniak
# długość wiadomości
