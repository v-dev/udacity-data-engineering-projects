# Project: Data Modeling with Postgres
First Udacity project for the [Data Engineering nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027).
Creates 5 tables in star schema design to represent songs played by users on a fictitious platform.

The tables are:
* `songplays` fact table
* `songs` dimension table
* `artists` dimension table
* `users` dimension table
* `time` dimension table


# Running
First, create the database and tables by running the `create_tables.py` python module:

    python create_tables.py

Then, run the ETL module to process the data and load the tables:

    python etl.py


# Directory and file structure
The `data/` directory contains source `.json` files for both song and user actions or events. It is subdivided into
`song_data` and `log_data` directories respectively, which are further grouped into song ID and event time directories.

Below are the 3 required python files:
* `sql_queries.py` : python variables holding DDL and DML for creating the various tables and populating them.
* `create_tables.py` : creates (or recreates) the tables using the DDL and DML in the `sql_queries.py`
* `etl.py` : main module that processes (does the ETL'ing of) the source `.json` files and loads them into the database.

> Note: The code to define `time_data` in `etl.py` uses `t.dt.week.values` to work in the Udacity workspace. However,
> when run on my Ubuntu 22.0.4 machine with Python 3.10.6, it generates a warning:
> 
>     FutureWarning: Series.dt.weekofyear and Series.dt.week have been deprecated. Please use Series.dt.isocalendar().week instead.
> 
> Therefore, you can swap that code line with the commented out line directly below it to remove the warning.

Below are practice and test Jupyter notebook files. They are not required, but helpful:
* `etl.ipynb` : Initial practice scripts with overall instructions to program the real ETL.
* `test.ipynb` : Scripts to check the created tables and their structure and contents for basic rubric correctness.
