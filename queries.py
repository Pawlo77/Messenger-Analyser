import pandas as pd
import json
from typing import Any, List
from datetime import datetime
from helpers import Config
from helpers import Query
import emoji
class CountMessagesQuery(Query):
    """
    Groups messages by conversation_id, date (rrrr-mm-dd hh:minmin:00) and returns
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
        with open(
            Config.get("output_dir_path")
            + "/"
            + Config.get("prefix")
            + "_users_reversed.json",
            "r",
            encoding="utf-8",
        ) as file:
            data_dict = json.load(file)
        groups = kwargs.pop("groups", None)
        assert groups is not None, f"Query: {self.id} requires groups to work."

        get_entry = lambda conversation_id, message: [conversation_id in groups] + [
            1 if len(message) >= num else 0 for num in self.min_messages_num
        ]

        counts = {}
        for conversation_id, user_id, message, timestamp in data:
            date = self.get_date(timestamp)
            key = (conversation_id, user_id)
            if key in counts.keys():
                if date in counts[key].keys():
                    for i, num in enumerate(self.min_messages_num):
                        if len(message) >= num:
                            counts[key][date][i + 1] += 1
                else:
                    counts[key][date] = get_entry(conversation_id, message)

            else:
                counts[key] = {date: get_entry(conversation_id, message)}

        df =  pd.DataFrame(
            [
                (outer_key[0], data_dict.get(outer_key[1])[0], inner_key, *inner_values)
                for outer_key, outer_values in counts.items()
                for inner_key, inner_values in outer_values.items()
            ],
            columns=["conversation_id", "user_id", "date", "is_group"]
            + [f"min_messages={num}" for num in self.min_messages_num],
        )
        df_grouped = df.groupby(['conversation_id', 'user_id', 'date', 'is_group']).agg({
                                'min_messages=0': 'sum',
                                'min_messages=3': 'sum',
                                'min_messages=7': 'sum',
                                'min_messages=15': 'sum'
                            }).reset_index()
        return df_grouped



class MostCommonStrings(Query):
    """
    Returning most common sequences of --words_count (default 2) words.
    Data frame has columns: 
    user - user --user_id, that send this,
    sequence of strings and count
    """
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
        how_many_words = Config.get("words_count")
        assert how_many_words in [1,2,3,4]
        for line in data:
            user_id, message = line[-3], line[-2]
            if user_id != Config.get("user_id"):
                continue
            if how_many_words == 1:
                for i in range(len(message)):
                    pair_user = message[i], user_id
                    if pair_user in df_dict:
                        df_dict[pair_user] += 1
                    else:
                        df_dict[pair_user] = 1
            elif how_many_words == 2:
                for i in range(len(message) - 1):
                    pair_user = message[i] + " " + message[i + 1], user_id
                    if pair_user in df_dict:
                        df_dict[pair_user] += 1
                    else:
                        df_dict[pair_user] = 1
            elif how_many_words == 3:
                for i in range(len(message) - 2):
                    pair_user = message[i] + " " + message[i + 1] + " " + message[i + 2], user_id
                    if pair_user in df_dict:
                        df_dict[pair_user] += 1
                    else:
                        df_dict[pair_user] = 1
            else:
                for i in range(len(message) - 3):
                    pair_user = message[i] + " " + message[i + 1] + " " + message[i + 2] + " " + message[i + 3], user_id
                    if pair_user in df_dict:
                        df_dict[pair_user] += 1
                    else:
                        df_dict[pair_user] = 1
        df = pd.DataFrame(
            [
                (data_dict.get(key[1])[0], key[0], value)
                for key, value in df_dict.items()
            ],
            columns=["user", "sequence of strings", "count"],
        )
        df = df.sort_values("count", ascending=False)
        return df

class TimeToResponde(Query):
    """
    Query, that calculate the time of root (--user_id) response and returns data frame with columns:
    sender - user_id, not root,
    time_send - time that user sent last massege before response
    time_response - time that root sent first message on response
    delta_times - difference between last message sent to first response
    times format - yyyy-mm-dd hh-minmin-00
    """
    def __init__(self) -> None:
        super().__init__(id="TimeToResponde", result_extension=".csv")

    def execute(self, data: List[tuple], **kwargs) -> Any:
        df_dict = {}
        root_id = Config.get("user_id")
        assert len(root_id) > 0
        
        queue = []
        groups = kwargs.pop("groups", None)
        assert groups is not None, f"Query: {self.id} requires groups to work."

        get_entry = lambda conversation_id: conversation_id in groups
        for line in data:
            conversation_id, user_id, timestamp = line[-4], line[-3],  line[-1]
            date = self.get_date(timestamp)
            if get_entry(conversation_id):
                continue
            # responde time
            if len(queue) != 0: # if we have message from root
                if user_id != queue[0] and user_id != root_id: # if user isn't root, calculate delta times and save to temp. dict
                    delta =  queue[1] - date
                    key = user_id, date
                    df_dict[key] = queue[1], delta
                    queue = []
                elif user_id == root_id: # else, we want to have first massege of response
                    queue = []
                    queue.append(user_id)
                    queue.append(date)
            else: # if queue is empty
                if user_id != root_id: # we don't want to have other masseges from non root user, exept last message
                    continue
                else:  # save message details from root
                    queue.append(user_id)
                    queue.append(date)
        
        
        # create data frame with user_id(sender not root), time_send, time_responde, delta times
        df = pd.DataFrame(
            [(key[0], key[1], value[0], value[1]) for key, value in df_dict.items()],
            columns=["sender", "time_send", "time_responde", "delta_times"],
        )
        return df
class MostCommonEmoji(Query):
    """
    Creates data frame contains: date of message sent, emoji that root sent(--user_id), words before and after emoji (if exist)
    and count of values. time in format: yyyy-mm-dd hh-minmin-00
    """
    def __init__(
        self
    ) -> None:
        super().__init__("emoji", ".csv")
    @staticmethod
    def get_users(data: List[tuple]):
        conversation_users = {}
        for conversation_id, user_id, _, timestamp in data:
            if conversation_id in conversation_users.keys():
                conversation_users[conversation_id].add(user_id)
            else:
                conversation_users[conversation_id] = set([user_id])
        return conversation_users

    def execute(self, data: List[tuple], **kwargs) -> Any:
        df_dict = {}    
        groups = kwargs.pop("groups", None)
        assert groups is not None, f"Query: {self.id} requires groups to work."
        
        with open(
            Config.get("output_dir_path")
            + "/"
            + Config.get("prefix")
            + "_users_reversed.json",
            "r",
            encoding="utf-8",
        ) as file:
            data_dict = json.load(file)
        root_id = Config.get("user_id")
        get_entry = lambda conversation_id: conversation_id in groups
        user = MostCommonEmoji.get_users(data)
        for line in data:
            conversation_id,  message ,timestamp = line[-4],line[-2],  line[-1]
            date = self.get_date(timestamp)           
            word_behind = ""
            word_next = ""
            if get_entry(conversation_id):
                gender = "group"
            else:
                us = user[conversation_id]
                for u in us:
                    if u != root_id:
                        gender = data_dict.get(u)[1]
            for i in range(len(message)):
                if emoji.is_emoji(message[i]):
                    key = (date, message[i], word_behind, word_next, gender)
                    if not key in df_dict:
                        df_dict[key] = 1
                    else:
                        df_dict[key] += 1
                if i == len(message) -1:
                    word_next = ""
                else:
                    word_next = message[i+1]
                word_behind = message[i]
        
        df = pd.DataFrame(
            [(key[0], key[1], key[2], key[3], key[4], value) for key, value in df_dict.items()],
            columns=["time", "emoji", "word_behind", "word_after", "gender_to_send", "count"],
        )
        return df
