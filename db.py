"""
Functions for interacting with a sqlite database to support the Fantasy Football module.
"""

# Standard library imports
import os
import sqlite3

# Third-party imports
import yaml

with open('_config.yml', 'r') as config_file:
    CONFIG = yaml.safe_load(config_file)


def connect():
    """
    Connects to the database containing player and stat info.
    :return: connection and cursor objects
    """
    db_path = os.path.normpath('F:/databases/nfl/players.db')
    conn = sqlite3.connect(db_path)
    # conn.set_trace_callback(print)
    conn.row_factory = dict_factory
    curs = conn.cursor()
    return conn, curs


def dict_factory(cursor, row):
    """
    Makes sqlite return an indexable dict of results rather than a tuple.
    Not called natively - just passed to the row_factory attribute.
    :return: n/a
    """
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d
