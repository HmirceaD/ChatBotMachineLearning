import sqlite3
import json
from datetime import datetime

comm_span = '2017-12'
sql_trans = []

conn = sqlite3.connect('{}.db'.format(comm_span))

curs = conn.cursor()

def create_table():
    curs.execute("""CREATE TABLE IF NOT EXISTS parent_reply(parent_id
                    TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT, comment TEXT,
                    subreddit TEXT, unix INT, score INT)""")

def format_data(data):
    dataz = data.replace('\n', ' newlinechar ').replace('\r', ' newlinechar ').replace('"', "'")
    return dataz

def find_parent(parent):

    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(parent)
        curs.execute(sql)

        result = curs.fetchone()

        if result != None:
            return result[0]
        else:
            return False

    except Exception as ex:
        print("EXECPTION: {}".format(ex))
        return False

def find_crr_score(parent):

    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(parent)
        curs.execute(sql)

        result = curs.fetchone()

        if result != None:
            return result[0]
        else:
            return False

    except Exception as ex:
        print("EXCEPTION: {}".format(ex))
        return False

def validate_data(data):

    if len(data.split(' ')) > 50 or len(data) < 1:
        return False
    elif len(data) > 1000:
        return False
    elif data is '[deleted]' or data is '[removed]':
        return False
    else:
        return True

def parse_sql():
    row_count = 0
    paired_row = 0

    with open("E:/DataSets/Reddit Comments 2017-Dec/{}".format(comm_span, comm_span), buffering = 1000) as f_obj:
        for row in f_obj:

            row_count += 1
            row = json.loads(row)

            #get all the necessary data
            parent_id = row['parent_id']
            body = format_data(row['body'])

            created_utc = row['created_utc']
            score = row['score']

            subreddit = row['subreddit']

            #find the parent to which they replied
            parent_data = find_parent(parent_id)



            """if score >= 2:
                crr_score = find_crr_score(parent_id)

                if crr_score:
                    if score > crr_score:"""



if __name__ == '__main__':
    create_table()
    parse_sql()

