import sqlite3
import json
import datetime

timeframe = "2015-01"
sql_transaction = []

# setting up connection
connection = sqlite3.connect(f"{timeframe}.db")
c = connection.cursor()

def create_table():
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS parent_reply (
            parent_id TEXT PRIMARY KEY,
            comment_id TEXT UNIQUE,
            parent TEXT,
            comment TEXT,
            subreddit TEXT,
            unix INT,
            score INT
        );
        """
    )

def format_data(data):
    data = data.replace("\n", " newlinechar ").replace("\r", " newlinechar ").replace('"', "'")
    return data

def find_parent(pid):
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = ? LIMIT 1"
        c.execute(sql, (pid,))
        result = c.fetchone()
        if result:
            return result[0]
        return False
    except Exception as e:
        print(f"Error finding parent: {str(e)}")
        return False

def find_existing_comment(pid):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = ? LIMIT 1"
        c.execute(sql, (pid,))
        result = c.fetchone()
        if result is not None:
            return result[0]
        return False
    except Exception as e:
        print(f"Error finding score: {str(e)}")
        return False

def acceptable_body(data):
    if len(data.split(' ')) > 50 or len(data) < 1:
        return False
    elif len(data) > 1000:
        return False
    elif data == '[deleted]' or data == '[removed]':
        return False
    else:
        return True

def sql_insert_replace(comment_id, parent_id, parent_data, body, subreddit, utc, score):
    try:
        sql = "UPDATE parent_reply SET comment_id = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? WHERE parent_id = ?"
        transaction_bldr(sql, (comment_id, parent_data, body, subreddit, utc, score, parent_id))
    except Exception as e:
        print(f"Error updating insert: {str(e)}")

def sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, utc, score):
    try:
        sql = "INSERT INTO parent_reply (comment_id, parent_id, parent, comment, subreddit, unix, score) VALUES (?, ?, ?, ?, ?, ?, ?)"
        transaction_bldr(sql, (comment_id, parent_id, parent_data, body, subreddit, utc, score))
    except Exception as e:
        print(f"Error inserting row HasParent: {str(e)}")

def sql_insert_no_parent(comment_id, parent_id, body, subreddit, utc, score):
    try:
        sql = "INSERT INTO parent_reply (comment_id, parent_id, comment, subreddit, unix, score) VALUES (?, ?, ?, ?, ?, ?)"
        transaction_bldr(sql, (comment_id, parent_id, body, subreddit, utc, score))
    except Exception as e:
        print(f"Error inserting row NoParent: {str(e)}")

def transaction_bldr(sql, values):
    global sql_transaction
    sql_transaction.append((sql, values))
    if len(sql_transaction) > 1000:
        c.execute('BEGIN TRANSACTION')
        for s, v in sql_transaction:
            try:
                c.execute(s, v)
            except Exception as e:
                # print("SQL error: " + str(e))
                pass
        connection.commit()
        sql_transaction = []

if __name__ == "__main__":
    create_table()
    row_counter = 0
    paired_rows = 0
    with open("C:\\Users\\nithi\\Downloads\\RC_2015-01\\RC_2015-01", buffering=1000) as f:
        for row in f:
            row_counter += 1
            row = json.loads(row)
            parent_id = row["parent_id"]
            comment_id = row["name"]
            body = format_data(row["body"])
            utc = row["created_utc"]
            score = row["score"]
            subreddit = row["subreddit"]

            parent_data = find_parent(parent_id)

            if score >= 2:
                if acceptable_body(body):
                    existing_comment_score = find_existing_comment(parent_id)
                    if existing_comment_score:
                        try:
                            existing_comment_score = int(existing_comment_score)
                            if score > existing_comment_score:
                                sql_insert_replace(comment_id, parent_id, parent_data, body, subreddit, utc, score)
                        except ValueError as e:
                            print(f"Error converting existing_comment_score to int: {str(e)}")
                    else:
                        if parent_data:
                            sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, utc, score)
                            paired_rows += 1
                        else:
                            sql_insert_no_parent(comment_id, parent_id, body, subreddit, utc, score)

            if row_counter % 100000 == 0 or row <100000:
                print(f"Total rows: {row_counter}, Paired rows: {paired_rows} Time: {str(datetime.datetime.now())}")

print("completed")