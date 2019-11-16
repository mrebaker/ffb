import json
import os

import sqlite3
import yaml
import yahoo_fantasy_api as yapi
from retry import retry
from yahoo_oauth import OAuth2


def authenticate():
    # auth = OAuth2(config['client_id'], config['client_secret'])
    auth = OAuth2(None, None, from_file='oauth.json')
    if not auth.token_is_valid():
        auth.refresh_access_token()
    return auth


def calc_week_stats():
    filename = os.path.normpath('data/nfl-weekstats-2019-10.json')
    with open(filename, 'r') as f:
        weekstats = json.load(f)

    player_stats = weekstats['players']

    oauth = authenticate()
    game = yapi.Game(oauth, 'nfl')
    league = game.to_league(config['league_id'])

    # initialise DB in case we need to map NFL and Yahoo names
    db_path = os.path.normpath('F:/databases/nfl/players.db')
    conn = sqlite3.connect(db_path)
    curs = conn.cursor()

    for team in league.teams():
        roster = league.to_team(team['team_key']).roster()
        print(team['name'])
        for player in roster:
            try:
                stats = next(item for item in player_stats if item['name'] == player['name'])
            except StopIteration:
                result = curs.execute('SELECT nfl_name FROM player WHERE yahoo_name = ?', (player['name'],)
                                            ).fetchone()
                if result is None:
                    stats = None
                else:
                    matched_name = result[0]
                    stats = next((item for item in player_stats if item['name'] == matched_name), None)

            if stats:
                print(f'{player["name"]}: {stats}')
            else:
                print(f'not found: {player}')
        print('-' * 15)


@retry(json.decoder.JSONDecodeError, delay=5, backoff=5, max_delay=7500)
def get_player(player_name):
    oauth = authenticate()
    league = yapi.Game(oauth, 'nfl').to_league(config['league_id'])
    return league.player_details(player_name)


def load_config():
    with open('config.yml', 'r') as f:
        conf = yaml.safe_load(f)
    return conf


def update_player_database():
    db_path = os.path.normpath('F:/databases/nfl/players.db')
    conn = sqlite3.connect(db_path)
    curs = conn.cursor()
    curs.execute('''CREATE TABLE IF NOT EXISTS player (
                    id integer PRIMARY KEY,
                    nfl_name text,
                    nfl_id text,
                    esbid text,
                    gsisPlayerId text,
                    yahoo_name text)''')

    filename = os.path.normpath('data/nfl-seasonstats-2019-10.json')
    with open(filename, 'r') as f:
        player_stats = json.load(f)['players']

    for player in player_stats:
        result = curs.execute('SELECT * FROM player WHERE nfl_id = ?', (player['id'],)).fetchall()
        if len(result) == 0:
            values = (player['name'],
                      player['id'],
                      player['esbid'],
                      player['gsisPlayerId'])

            curs.execute('''INSERT INTO player (nfl_name, nfl_id, esbid, gsisPlayerId)
                            VALUES (?, ?, ?, ?)''', values)
            conn.commit()

    # get Yahoo league to query name
    oauth = authenticate()
    league = yapi.Game(oauth, 'nfl').to_league(config['league_id'])

    players = curs.execute('SELECT id, nfl_name, yahoo_name, yahoo_id FROM player').fetchall()

    # add Yahoo ID if missing
    for player in players:
        print(player)
        db_id, nfl_name, yahoo_name, yahoo_id = player
        if yahoo_id is None:
            player = get_player(nfl_name)
            # print(f'Unable to search player name "{nfl_name}"')

            try:
                yahoo_id = player['player_id']
            except TypeError:
                print(f'Unable to match NFL player name "{nfl_name}" to a Yahoo player name.')
                pass

            values = (yahoo_id, nfl_name)
            curs.execute('''UPDATE player
                            SET yahoo_id = ?
                            WHERE nfl_name = ?''', values)
            conn.commit()


if __name__ == '__main__':
    config = load_config()
    update_player_database()
    # calc_week_stats()
