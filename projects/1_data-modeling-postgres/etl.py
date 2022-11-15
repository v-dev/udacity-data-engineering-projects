import glob
import os

import pandas as pd
import psycopg2

from sql_queries import *


def process_song_file(cur, filepath):
    """
    Populate song and artist tables using given song data/JSON files.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude',
                           'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def create_time_dict(time_data, column_labels):
    """
    Create a dictionary of time data and column labels in order to create a `time` dataframe.
    """
    result = {}
    for i in range(0, len(time_data)):
        result[column_labels[i]] = time_data[i]
    return result


def process_log_file(cur, filepath):
    """
    Populate time, user, and songplay tables using given log data/JSON files.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = [t.values, t.dt.hour.values, t.dt.day.values,
                 t.dt.week.values, t.dt.month.values,
                 # use to get rid of "FutureWarning"s - t.dt.isocalendar().week.values, t.dt.month.values,
                 t.dt.year.values, t.dt.weekday.values]
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    timedict = create_time_dict(time_data, column_labels)
    time_df = pd.DataFrame(timedict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid,
                         row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Traverses given filepath and executes given function on JSON files located there.

    Process either song or log data depending on given filepath and function.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Main entrypoint for running the ETL script.

    Connects to the sparkifydb to extract the song and log JSON files
    and transforms/loads them into 5 star schema tables.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
