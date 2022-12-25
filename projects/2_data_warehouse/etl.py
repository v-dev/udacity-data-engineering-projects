import configparser
import datetime

import psycopg2

from sql_queries import copy_table_queries, insert_table_queries, STG_EVENTS_TABLE, STG_SONGS_TABLE, SONGPLAYS_TABLE, \
    USERS_TABLE, SONGS_TABLE, ARTISTS_TABLE, TIME_TABLE


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(f"[{datetime.datetime.now()}] about to execute query: {query}")
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(f"[{datetime.datetime.now()}] about to execute query: {query}")
        cur.execute(query)
        conn.commit()


def select_count(cursor, table_name):
    cursor.execute(f"select count(1) from {table_name}")
    print(f"[{datetime.datetime.now()}] count of '{table_name}': {cursor.fetchall()[0][0]}")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    select_count(cur, 'vstudent_temp')

    load_staging_tables(cur, conn)
    select_count(cur, STG_EVENTS_TABLE)  # expecting: 8,056
    select_count(cur, STG_SONGS_TABLE)  # expecting: 14,896

    insert_tables(cur, conn)
    select_count(cur, SONGPLAYS_TABLE)  # expecting: 1144
    select_count(cur, SONGS_TABLE)  # expecting: 14,896
    select_count(cur, ARTISTS_TABLE)  # expecting: 14,896
    select_count(cur, USERS_TABLE)  # expecting: 7,770
    select_count(cur, TIME_TABLE)  # expecting: 8,056

    conn.close()


if __name__ == "__main__":
    main()
