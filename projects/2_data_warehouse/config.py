import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')

HOST = config.get("CLUSTER", "HOST")
DB_NAME = config.get("CLUSTER", "DB_NAME")
DB_USER = config.get("CLUSTER", "DB_USER")
DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
DB_PORT = int(config.get("CLUSTER", "DB_PORT"))
PSYCOPG2_CONNECT_STRING = f"host={HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} port={DB_PORT}"

ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
