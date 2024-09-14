import sqlite3
import pandas as pd
import concurrent.futures

timeframes = ['2015-05']
batch_size = 10000
last_unix_file = 'last_unix.txt'

def write_last_unix_to_file(last_unix):
    with open(last_unix_file, 'w') as f:
        f.write(str(last_unix))

def read_last_unix_from_file():
    try:
        with open(last_unix_file, 'r') as f:
            a =int(f.read().strip())
            print(a)
            return a
        
    except FileNotFoundError:
        return 0

def process_batch(timeframe, last_unix, test_done):
    connection = sqlite3.connect(f'{timeframe}.db')
    df = pd.read_sql(f"SELECT * FROM parent_reply WHERE unix > {last_unix} AND parent IS NOT NULL AND score > 2 ORDER BY unix ASC LIMIT {batch_size}", connection)
    connection.close()
    
    if df.empty:
        return None, test_done

    last_unix = df.tail(1)['unix'].values[0]
    parent_data = '\n'.join(df["parent"].values) + '\n'
    comment_data = '\n'.join(df["comment"].values) + '\n'

    if not test_done:
        with open("test.from", 'a', encoding='utf8') as f:
            f.write(parent_data)
        with open("test.to", 'a', encoding='utf8') as f:
            f.write(comment_data)
        test_done = True
    else:
        with open("train.from", 'a', encoding='utf8') as f:
            f.write(parent_data)
        with open("train.to", 'a', encoding='utf8') as f:
            f.write(comment_data)

    write_last_unix_to_file(last_unix)
    return last_unix, test_done

def main():
    for timeframe in timeframes:
        last_unix = read_last_unix_from_file()
        test_done = False
        counter = 0

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            while True:
                futures.append(executor.submit(process_batch, timeframe, last_unix, test_done))
                if len(futures) >= 20:
                    for future in concurrent.futures.as_completed(futures):
                        result = future.result()
                        if result is None:
                            break
                        last_unix, test_done = result
                        counter += 1
                        if counter % 20 == 0:
                            print(counter * batch_size, "rows completed so far")
                    futures = []

            # Process any remaining futures
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result is None:
                    break
                last_unix, test_done = result
                counter += 1
                if counter % 20 == 0:
                    print(counter * batch_size, "rows completed so far")

if __name__ == "__main__":
    main()