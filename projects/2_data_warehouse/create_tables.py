import psycopg2

import config
from dwh_utils import log_query
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        log_query(query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        log_query(query)
        cur.execute(query)
        conn.commit()


def main():
    conn = psycopg2.connect(config.PSYCOPG2_CONNECT_STRING)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
