"""
Functions for interacting with a sqlite database to support the Fantasy Football module.
"""

# Standard library imports
import fnmatch
import json
import os
import re
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


def load_nfl_game_data():
    """
    Runs through every stat file in the data folder and uploads the weekly player/game data to the
    database.
    """
    # TODO - should check for duplicates first!
    folder = 'data_in'
    files = []
    for (_, _, file_names) in os.walk(folder):
        files.extend(file_names)
        break
    stat_files = fnmatch.filter(files, '*weekstats*.json')
    conn, curs = connect()
    for stat_file in stat_files:
        season = re.split('[-\.]', stat_file)[2]
        week = re.split('[-\.]', stat_file)[3]
        with open(os.path.join(folder, stat_file), 'r') as f:
            stats = json.loads(f.read())
            players = stats['games']['102019']['players']
            for player_id, player_stats in players.items():
                stats = player_stats['stats']['week'][season][week]
                for k, v in stats.items():
                    params = (player_id, season, week, k, v)
                    curs.execute('''INSERT INTO weekstat
                                    (player_nfl_id, season, week, stat_nfl_id, stat_vol)
                                    VALUES
                                    (?, ?, ?, ?, ?)''', params)
                conn.commit()
