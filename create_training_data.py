
import sqlite3
import pandas as pd


timeframes = ['2015-05']

for timeframe in timeframes:
    connection = sqlite3.connect(f'{timeframe}.db')
    c = connection.cursor()
    limit = 5000 
    last_unix = 0
    cur_length = limit
    counter = 0
    test_done = False
    while cur_length == limit:
        df = pd.read_sql(f"SELECT * FROM parent_reply where unix > {last_unix} and parent not null and score  > 2 order by unix asc limit {limit}",connection)
        last_unix = df.tail(1)['unix'].values[0]
        cur_length = len(df)
        if not test_done:
            with open("test.from",'a', encoding='utf8') as f:
                for content in df["parent"].values:
                    f.write(content+'\n')
            with open("test.to",'a', encoding='utf8') as f:
                for content in df["comment"].values:
                    f.write(content+'\n')
            test_done = True
        else:
            with open("train.from",'a', encoding='utf8') as f:
                for content in df["parent"].values:
                    f.write(content+'\n')
            with open("train.to",'a', encoding='utf8') as f:
                for content in df["comment"].values:
                    f.write(content+'\n')
            test_done = True
            counter +=1
            if counter%20 == 0:
                print(counter*limit,"rows completed so far")