from datetime import datetime


def log_query(query):
    print(f"[{datetime.now()}] about to execute query: {query}")
