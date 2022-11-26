# Project 1B: Data Modeling with Cassandra
Second project for Udacity Data Engineering Course. This one is for data modeling using Cassandra.

# Directory and file structure
The main project code to turn in, per instructions, is a Jupyter notebook, `project1b_etl.ipynb`.
It imports 3 supporting modules for the different tables created, for the 3 different queries:
1. `songs_by_session.py`
2. `songs_by_player.py`
3. `players_by_song.py`

# Extract `event_data` directory
Run the following to unzip the `event_data` directory:

    tar zxf event_data.tar.gz

# Pytest
The project has a Pytest file, `project1b_test.py`, to validate the tables and data. It can be run from
the same directory as this readme, like so:

    pytest -rA
