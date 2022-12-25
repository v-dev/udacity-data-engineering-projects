import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def get_config_data():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    return {
        'host': config.get("CLUSTER","HOST"),
        'dbname': config.get("CLUSTER","DB_NAME"),
        'user': config.get("CLUSTER","DB_USER"),
        'password': config.get("CLUSTER","DB_PASSWORD"),
        'port': int(config.get("CLUSTER","DB_PORT")),

        # TODO: possibly not needed here and can be removed
        'arn': config.get("IAM_ROLE","ARN"),
        'log_data': config.get("S3","LOG_DATA"),
        'song_data': config.get("S3","SONG_DATA")
    }


def main():
    data = get_config_data()
    conn = psycopg2.connect(f"host={data['host']} dbname={data['dbname']} user={data['user']} password={data['password']} port={data['port']}")
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
