import pandas as pd
import json
from typing import Any, List
from datetime import datetime
from helpers import Config
from helpers import Query


class CountMessagesQuery(Query):
    def __init__(
        self,
        min_messages_num: List[int] = [0, 3, 7, 15],
    ) -> None:
        super().__init__("count_messages", ".csv")
        self.min_messages_num = min_messages_num
    @staticmethod
    def update_valueT(value):
        return {'val':value, 'group': True}
    @staticmethod
    def update_valueF(value):
        return {'val': value, 'group': False}
    

    def execute(self, data: List[tuple], **kwargs) -> Any:
        dates = {}
        assert len(data) > 0
        title0 = data[0][0]
        user_count = 0
        users = []
        group = False
        pom = {}
        for line in data:
            title, user_id, message, timestamp = line[-4], line[-3], line[-2], line[-1]
            date = self.get_date(timestamp)
            if title != title0:
                if group:
                   pom = dict(map(lambda kv: (kv[0], MessagesOnTime.update_valueT(kv[1])), pom.items()))
                else:
                    pom = dict(map(lambda kv: (kv[0], MessagesOnTime.update_valueF(kv[1])), pom.items()))
                pom.update(dates)
                dates = pom
                pom = {}
                user_count = 0
                users = []
                title0 = title
                group = False
            if user_id not in users and user_count < 3:
                user_count+=1
                users.append(user_id)
            if title == title0 and user_count > 2:
                group = True
                
                    
            if date not in pom:
                pom[date] = {str(key): 0 for key in self.min_messages_num}


            for min_message_num in self.min_messages_num:
                if len(message) > min_message_num:
                    pom[date][str(min_message_num)] += 1
        
        pom.update(dates)
        dates = pom
        df = pd.DataFrame([(key[0], value) for key, value in dates.items()], columns=['date', 'count'])
        df['group'] = df['count'].apply(lambda x: x['group'])
        df = pd.concat([df, df['count'].apply(pd.Series)], axis=1)
        df.drop(['count'], axis=1, inplace=True)
        dfdf = pd.DataFrame(df['val'])


        all_keys = set().union(*(d.keys() for d in dfdf['val']))

        result_dict = {key: [row.get(key, None) for row in dfdf['val']] for key in all_keys}
        result_df = pd.DataFrame(result_dict)
        df = pd.concat([df, result_df], axis=1)
        df.drop('val', axis=1, inplace=True)
        df = df[['date','group','0','3','7', '15']]

        return df
class MessagesOnTime(Query):
    def __init__(
        self
    ) -> None:
        super().__init__(id="messageontime", result_extension=".csv")
    @staticmethod
    def update_valueT(value):
        return {'val': value, 'group': True}
    @staticmethod
    def update_valueF(value):
        return {'val': value, 'group': False}

    def execute(self, data: List[tuple], **kwargs) -> Any:
        df_dict = {}
        with open(Config.get("output_dir_path") + "/" + Config.get("prefix") + '_users_reversed.json', 'r', encoding='utf-8') as file:
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
                   pom = dict(map(lambda kv: (kv[0], MessagesOnTime.update_valueT(kv[1])), pom.items()))
                else:
                    pom = dict(map(lambda kv: (kv[0], MessagesOnTime.update_valueF(kv[1])), pom.items()))


                pom.update(df_dict)
                df_dict = pom
                pom = {}
                user_count = 0
                users = []
                title0 = title
                group = False

            if user_id not in users and user_count < 3:
                user_count+=1
                users.append(user_id)
            if title == title0 and user_count > 2:
                group = True
                
            key = (date, gender)
            if key not in pom:
                pom[key] = 1
            
        if group:
            pom = dict(map(lambda kv: (kv[0], MessagesOnTime.update_valueT(kv[1])), pom.items()))
        else:
            pom = dict(map(lambda kv: (kv[0], MessagesOnTime.update_valueF(kv[1])), pom.items()))
        pom.update(df_dict)
        df_dict = pom
        df = pd.DataFrame([(key[0].replace(second = 0, microsecond = 0), key[1], value['val'],value['group']) for key, value in df_dict.items()], columns=['date', 'gender', 'count', 'group'])

        return df
class Top10withoutGroups(Query):
    def __init__(
        self
    ) -> None:
        super().__init__(id="top10mess", result_extension=".csv")
    
    def execute(self, data: List[tuple], **kwargs) -> Any:
        df_dict = {}
        with open(Config.get("output_dir_path") + "/" + Config.get("prefix") + '_users_reversed.json', 'r', encoding='utf-8') as file:
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
                user_count+=1
                users.append(user_id)
            if title == title0 and user_count > 2:
                group = True
            if user_id not in pom:
                pom[user_id] = {'val': 1, 'user': data_dict.get(user_id)[0]}
            else:
                pom[user_id]['val'] += 1
        if not group:
            pom.update(df_dict)
            df_dict = pom
        df = pd.DataFrame([(value['user'], value['val']) for key, value in df_dict.items()], columns=['user', 'count'])
        df = df.sort_values(by='count', ascending=False).head(10)

        return df

# najczestrze słowa idące w parze/trójce - pewniak
# długość wiadomości
# top 10 osob bez grup