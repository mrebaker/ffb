"""
Functions for interacting with a sqlite database to support the Fantasy Football module.
"""

# Standard library imports
import fnmatch
import json
import logging
import os
import re
import sqlite3

# Third-party imports
from tqdm import tqdm
import yaml

# Local imports
import api

with open('_config.yml', 'r') as config_file:
    CONFIG = yaml.safe_load(config_file)

log = logging.getLogger()
logging.basicConfig(filename='ffb.log', level=logging.DEBUG)


def build_database():
    """
    Reconstructs the player, stat and game database from scratch.
    """
    # db_folder = os.path.normpath('F:/Databases/nfl')
    # db_filename = 'players.db'
    #
    # if os.path.isfile(os.path.join(db_folder, db_filename)) or \
    #         os.path.isfile(os.path.join(db_folder, db_filename + '-journal')):
    #     raise RuntimeError('Remove or rename existing database file(s) before proceeding.')
    #
    # update_player_data()
    # update_stats_data()
    load_nfl_game_data()


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


def latest_game_data():
    _, curs = connect()
    row = curs.execute('''SELECT season, week FROM weekstat
                          ORDER BY season desc, week desc
                          LIMIT 1''').fetchone()
    print(row)


def load_nfl_game_data():
    """
    Runs through every stat file in the data folder and uploads the weekly player/game data to the
    database.
    """
    conn, curs = connect()
    curs.execute('''CREATE TABLE IF NOT EXISTS weekstat (
                    id INTEGER PRIMARY KEY,
                    player_nfl_id TEXT,
                    season INTEGER,
                    week INTEGER,
                    stat_nfl_id TEXT,
                    stat_vol REAL)''')
    conn.commit()

    folder = 'data_in'
    files = []
    for (_, _, file_names) in os.walk(folder):
        files.extend(file_names)
        break
    stat_files = fnmatch.filter(files, '*weekstats*.json')

    for stat_file in tqdm(stat_files):
        season = re.split('[-.]', stat_file)[2]
        week = re.split('[-.]', stat_file)[3]
        row = curs.execute('''SELECT * FROM weekstat
                              WHERE season = ? and week = ?
                              LIMIT 1''', (season, week)).fetchone()
        if row:
            continue

        with open(os.path.join(folder, stat_file), 'r') as f:
            stats = json.loads(f.read())
            players = stats['games']['102019']['players']
            for player_id, player_stats in tqdm(players.items()):
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

    # add missing players from the NFL stat data
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

    # add Yahoo ID if missing
    yahoo_players = api.players()
    for player in yahoo_players:
        yahoo_name = player['name']['full']
        yahoo_id = player['player_id']
        eligible_positions = player['eligible_positions'][0]['position']

        db_players = curs.execute('''SELECT id, nfl_name, yahoo_name, yahoo_id, eligible_positions
                                    FROM player
                                    WHERE nfl_name = ?''', (yahoo_name,)).fetchall()

        if not db_players:
            log.info(f'No player in database called {yahoo_name}')
            continue

        if len(db_players) > 1:
            log.info(f'{len(db_players)} instance(s) found for {yahoo_name}')
            continue

        db_player = db_players[0]
        params = (yahoo_id, yahoo_name, eligible_positions, db_player['id'])
        curs.execute('''UPDATE player
                        SET yahoo_id = ?, yahoo_name = ?, eligible_positions = ?
                        WHERE id = ?''', params)
        conn.commit()

    # try screen scraping info where missing
    db_players = curs.execute('''SELECT id, nfl_name, yahoo_name, yahoo_id, eligible_positions
                                        FROM player
                                        WHERE yahoo_name IS NULL
                                        or yahoo_id IS NULL
                                        or eligible_positions IS NULL''').fetchall()

    for player in db_players:
        scraped_player = api.scrape_player(player['nfl_name'])
        if not scraped_player:
            if '.' in player['nfl_name']:
                scraped_player = api.scrape_player(player['nfl_name'].replace('.', ''))
                if not scraped_player:
                    continue
            else:
                continue

        data_points = []
        for data_point in ['full_name', 'id', 'position']:
            try:
                data_points.append(scraped_player[data_point])
            except KeyError:
                data_points.append(None)
                continue

        if data_points[1]:
            data_points[1] = data_points[1].split('.')[-1]

        data_points.append(player['id'])

        values = tuple(data_points)
        curs.execute('''UPDATE player
                        SET (yahoo_name, yahoo_id, eligible_positions)
                         = (?, ?, ?)
                        WHERE id = ?''', values)
        conn.commit()

    # fix Yahoo IDs for DST - screen scraping returns xx where it should be 1000xx
    # these all have Yahoo IDs between 1 and 35
    db_dst = curs.execute('''SELECT * FROM player
                             WHERE nfl_id BETWEEN 100001 AND 100032''').fetchall()

    for dst in db_dst:
        curs.execute('''UPDATE player
                        SET eligible_positions = "DEF"
                        WHERE id = ?''', (dst['id'],))

        yahoo_id = int(dst['yahoo_id'])
        if 0 < yahoo_id < 35:
            new_id = f'1000{yahoo_id:02}'
            curs.execute('''UPDATE player
                            SET yahoo_id = ?
                            WHERE id = ?''', (new_id, dst['id']))
        conn.commit()


def update_stats_data():
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


def calc_player_weekly_points():
    conn, curs = connect()
    curs.execute('''DROP TABLE IF EXISTS player_weekly_points''')
    curs.execute('''CREATE TABLE player_weekly_points AS
                    SELECT weekstat.player_nfl_id, weekstat.season, weekstat.week, 
                    sum(weekstat.stat_vol*statline.points) as points
                    FROM weekstat LEFT JOIN statline on weekstat.stat_nfl_id=statline.nfl_id
                    WHERE statline.points IS NOT NULL
                    GROUP BY weekstat.player_nfl_id, weekstat.season, weekstat.week
                    ''')
    conn.commit()
