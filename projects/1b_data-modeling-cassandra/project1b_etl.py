"""
inspired from "proj-1b-template.ipynb" but skipping the Extract part of reading
individual CSVs into final CSV since i just have the final CSV already / only

uses separate modules for the 3 tables to reference their SQL scripts
"""

import csv
import glob
import os
import time

from cassandra.cluster import Cluster

import players_by_song
import songs_by_player
import songs_by_session

keyspace = "udacity"
final_csv = 'event_datafile_new.csv'


def create_filepaths():
    """
    copied from template notebook: create a list of csv files to loop through
    """
    filepath = os.getcwd() + '/event_data'

    for root, dirs, files in os.walk(filepath):
        file_path_list = glob.glob(os.path.join(root, '*'))
    return file_path_list


def create_final_csv(file_path_list):
    """
    copied from template notebook: create smaller, final CSV for inserting rows
    """
    full_data_rows_list = []

    for f in file_path_list:
        with open(f, 'r', encoding='utf8', newline='') as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader)
            for line in csvreader:
                full_data_rows_list.append(line)

    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open(final_csv, 'w', encoding='utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist', 'firstName', 'gender', 'itemInSession', 'lastName', 'length',
                         'level', 'location', 'sessionId', 'song', 'userId'])
        for row in full_data_rows_list:
            if row[0] == '':
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

    with open('event_datafile_new.csv', 'r', encoding='utf8') as f:
        final_count = sum(1 for line in f)

    assert final_count == 6821


def recreate_keyspace(session):
    """
    create keyspace, if necessary
    """
    create_keyspace_command = f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH REPLICATION = "
    create_keyspace_command += "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"
    session.execute(create_keyspace_command)


def use_keyspace(session):
    """
    set the session keyspace
    """
    session.set_keyspace(keyspace)


def drop_tables(session):
    """
    drop tables, if they exist
    """
    start = time.perf_counter()
    session.execute(songs_by_session.drop)
    session.execute(songs_by_player.drop)
    session.execute(players_by_song.drop)
    stop = time.perf_counter()
    print(f"drop_tables in {stop - start:0.4f} seconds")


def create_tables(session):
    """
    (re)create tables
    """
    start = time.perf_counter()
    session.execute(songs_by_session.create)
    session.execute(songs_by_player.create)
    session.execute(players_by_song.create)
    stop = time.perf_counter()
    print(f"create_tables in {stop - start:0.4f} seconds")


def populate_tables(session):
    """
    loop through all the rows in the combined CSV, `final_csv`, and populate tables with their data

    Note: not all fields in the CSV are currently used
    """
    start = time.perf_counter()
    with open(final_csv, encoding='utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader)  # skip header
        for line in csvreader:
            artist_name = line[0]
            first_name = line[1]
            gender = line[2]
            item_in_session = int(line[3])
            last_name = line[4]
            length = float(line[5])
            level = line[6]
            location = line[7]
            session_id = int(line[8])
            song = line[9]
            user_id = int(line[10])
            session.execute(songs_by_session.insert, (artist_name, song, length, session_id, item_in_session))
            session.execute(songs_by_player.insert,
                            (artist_name, song, f"{first_name} {last_name}", user_id, session_id, item_in_session))
            session.execute(players_by_song.insert, (song, user_id, f"{first_name} {last_name}"))

        stop = time.perf_counter()
        print(f"inserts in {stop - start:0.4f} seconds")


def main():
    """
    Main, or starting entrypoint for ETL process.

    Re-reates keyspace and tables, then populates them using the final CSV.
    """
    cluster = None
    session = None

    # filepaths = create_filepaths()
    # create_final_csv(filepaths)

    try:
        cluster = Cluster(['127.0.0.1'])
        session = cluster.connect()

        recreate_keyspace(session)
        use_keyspace(session)
        drop_tables(session)
        create_tables(session)
        populate_tables(session)

    except Exception as e:
        print(f"Exception:\n{e}")
    finally:
        session.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    main()
