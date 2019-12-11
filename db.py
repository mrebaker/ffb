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

# Local imports
import api

with open('_config.yml', 'r') as config_file:
    CONFIG = yaml.safe_load(config_file)


def build_database():
    """
    Reconstructs the player, stat and game database from scratch.
    """
    db_folder = os.path.normpath('F:/Databases/nfl')
    db_filename = 'players.db'
    #
    # if os.path.isfile(os.path.join(db_folder, db_filename)) or \
    #         os.path.isfile(os.path.join(db_folder, db_filename + '-journal')):
    #     raise RuntimeError('Remove or rename existing database file(s) before proceeding.')

    update_player_data()
    # update_stats_database()
    # load_nfl_game_data()


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


def update_player_data():
    """
    Adds players from a week stat file, if missing from the database.
    :return:
    """
    conn, curs = connect()

    filename = os.path.normpath('data_in/nfl-seasonstats-2019-10.json')
    with open(filename, 'r') as f:
        player_stats = json.load(f)['players']

    curs.execute('''CREATE TABLE IF NOT EXISTS player (
                            id integer PRIMARY KEY,
                            nfl_name text,
                            nfl_id text,
                            esbid text,
                            gsisPlayerId text,
                            yahoo_name text,
                            yahoo_id text,
                            eligible_positions text)''')

    for player in player_stats:
        result = curs.execute('SELECT * FROM player WHERE nfl_id = ?', (player['id'],)).fetchall()
        if not result:
            values = (player['name'],
                      player['id'],
                      player['esbid'],
                      player['gsisPlayerId'])

            curs.execute('''INSERT INTO player (nfl_name, nfl_id, esbid, gsisPlayerId)
                            VALUES (?, ?, ?, ?)''', values)
            conn.commit()

    players = curs.execute('''SELECT id, nfl_name, yahoo_name,
                              yahoo_id, eligible_positions FROM player''').fetchall()

    for player in players:
        # add Yahoo details if missing
        if player['yahoo_id'] is None or player['yahoo_name'] is None:
            player_yahoo_profile = api.player(player['nfl_name'])
            try:
                yahoo_id = player_yahoo_profile['player_id']
                yahoo_name = player['nfl_name']
            except (TypeError, KeyError):
                scraped_player = api.scrape_player(player['nfl_name'])

                if not scraped_player:
                    if '.' in player['nfl_name']:
                        scraped_player = api.scrape_player(player['nfl_name'].replace('.', ''))
                        if not scraped_player:
                            continue
                    else:
                        continue

                yahoo_id = scraped_player['id'].split('.')[-1]
                yahoo_name = scraped_player['display_name']

            values = (yahoo_id, yahoo_name, player['nfl_name'])
            curs.execute('''UPDATE player
                            SET yahoo_id = ?, yahoo_name = ?
                            WHERE nfl_name = ?''', values)
            conn.commit()

        # add eligible positions if missing
        if player['eligible_positions'] is None:
            if player['yahoo_name'] is None:
                continue
            player_yahoo_profile = api.player(player['yahoo_name'])
            try:
                eligible_positions = player_yahoo_profile['eligible_positions']
                # Yahoo API returns a list of dicts, so extract the dict values
                position_list = [d['position'] for d in eligible_positions]
                position_text = ",".join(position_list)
            except TypeError:
                scraped_player = api.scrape_player(player['nfl_name'])
                if not scraped_player:
                    if '.' in player['nfl_name']:
                        scraped_player = api.scrape_player(player['nfl_name'].replace('.', ''))
                        if not scraped_player:
                            continue
                    else:
                        continue
                try:
                    position_text = scraped_player['positions']
                except KeyError:
                    continue

            values = (position_text, player['id'])
            curs.execute('''UPDATE player
                            SET eligible_positions = ?
                            WHERE id = ?''', values)
            conn.commit()


def update_stats_database():
    """
    Adds new stat types to the database.
    :return: nothing
    """
    db_path = os.path.normpath('F:/databases/nfl/players.db')
    conn = sqlite3.connect(db_path)
    curs = conn.cursor()
    curs.execute('''CREATE TABLE IF NOT EXISTS statline (
                        id integer PRIMARY KEY,
                        nfl_name text,
                        nfl_id text,
                        yahoo_name text,
                        yahoo_id text,
                        points real)''')

    nfl_stats_file = os.path.normpath('data_in/nfl-stats.json')

    with open(nfl_stats_file, 'r') as f:
        statlines = json.load(f)

    for stat_dict in statlines['stats']:
        vals = (stat_dict['name'],
                stat_dict['id'],
                stat_dict["id"],
                stat_dict["id"],
                stat_dict["id"])

        curs.execute("""insert or replace into statline
                        (nfl_name, nfl_id, yahoo_name, yahoo_id, points)
                        values (?,
                                ?, 
                                (SELECT yahoo_name from statline where nfl_id = CAST(? as text)),
                                (SELECT yahoo_id from statline where nfl_id = CAST(? as text)),
                                (SELECT points from statline where nfl_id = CAST(? as text))
                                );""", vals)
        conn.commit()

