from datetime import datetime as dt
import json
import os
import urllib.parse

import requests
import sqlite3
import yaml
import yahoo_fantasy_api as yapi

from retry import retry
from yahoo_oauth import OAuth2


class PotentialRateLimitError(BaseException):
    pass


def authenticate():
    # auth = OAuth2(config['client_id'], config['client_secret'])
    auth = OAuth2(None, None, from_file='oauth.json')
    if not auth.token_is_valid(print_log=False):
        auth.refresh_access_token()
    return auth


def calc_week_stats():
    score_file = os.path.normpath('data/nfl-weekstats-2019-10.json')

    with open(score_file, 'r') as f:
        week_stats = json.load(f)

    player_stats = week_stats['players']

    oauth = authenticate()
    game = yapi.Game(oauth, 'nfl')
    league = game.to_league(config['league_id'])

    # initialise DB in case we need to map NFL and Yahoo names
    db_path = os.path.normpath('F:/databases/nfl/players.db')
    conn = sqlite3.connect(db_path)
    conn.row_factory = dict_factory
    curs = conn.cursor()

    team_scores = {}
    for team in league.teams():
        roster = league.to_team(team['team_key']).roster(week=10)
        scores = {}
        missing_players = []
        for player in roster:
            if player['selected_position'] in ['BN', 'IR']:
                continue
            try:
                stats = next(item for item in player_stats if item['name'] == player['name'])
            except StopIteration:
                result = curs.execute('SELECT nfl_name FROM player WHERE yahoo_id = ?', (player['player_id'],)
                                            ).fetchone()
                if result is None:
                    stats = None
                else:
                    matched_name = result['nfl_name']
                    stats = next((item for item in player_stats if item['name'] == matched_name), None)

            if not stats:
                missing_players.append(player["name"])
                continue

            for k, v in stats['stats'].items():
                if k not in scores.keys():
                    scores[k] = int(v)
                else:
                    scores[k] += int(v)

        if missing_players:
            print(f'{team["name"]} missing players: {", ".join(missing_players)}')

        team_scores[team['name']] = scores

    stat_modifiers = curs.execute('SELECT * FROM statline').fetchall()

    for team, scores in team_scores.items():
        points = 0
        for stat, value in scores.items():
            try:
                multiplier = next(i['points'] for i in stat_modifiers if i['nfl_id'] == stat)
            except StopIteration:
                print(f'Couldn\'t find stat with NFL ID {stat} in the database. Defaulting to 0 points.')
                multiplier = 0

            if multiplier is None:
                print(f'Found stat with NFL ID {stat} in the database, but value found was None. Defaulting to 0 points.')
                multiplier = 0

            points += value * multiplier

        print(f'{team}: {points:.2f}')


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def find_players_by_score_type(nfl_score_id):
    score_file = os.path.normpath('data/nfl-weekstats-2019-10.json')

    with open(score_file, 'r') as f:
        week_stats = json.load(f)

    filtered = [i for i in week_stats['players'] if nfl_score_id in i['stats'].keys()]

    for player in filtered:
        attr_to_show = [player['name'],
                        player['teamAbbr'],
                        player['position'],
                        player['stats'][nfl_score_id]]

        print('\t'.join(attr_to_show))


@retry(PotentialRateLimitError, delay=5, backoff=4, max_delay=250)
def get_player(p_name):
    if "\'" in p_name:
        log(f'Unable to search player name {p_name}')
        return []
    oauth = authenticate()
    league = yapi.Game(oauth, 'nfl').to_league(config['league_id'])
    try:
        details = league.player_details(p_name)
    except json.decoder.JSONDecodeError as e:
        log('Potential rate limit error')
        print(f'Waiting for player {p_name}... ')
        raise PotentialRateLimitError
    return details


def get_league():
    oauth = authenticate()
    league = yapi.Game(oauth, 'nfl').to_league(config['league_id'])
    for stat in league.stat_categories():
        print(stat)


def load_config():
    with open('config.yml', 'r') as f:
        conf = yaml.safe_load(f)
    return conf


def log(msg):
    log_file = 'log.txt'
    with open(log_file, 'a+') as f:
        f.write(f'{dt.now().strftime("%Y-%m-%d %H:%M:s")} {msg}\n')


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

    for player in players:
        db_id, nfl_name, yahoo_name, yahoo_id = player

        # add Yahoo ID if missing
        if yahoo_id is None or yahoo_name is None:
            player = get_player(nfl_name)
            try:
                yahoo_id = player['player_id']
                yahoo_name = nfl_name
            except TypeError:
                log(f'Unable to match NFL player name "{nfl_name}" to a Yahoo player name.')
                log('Trying screen scrape')
                scraped_player = scrape_player(nfl_name)

                if not scraped_player:
                    if '.' in nfl_name:
                        scraped_player = scrape_player(nfl_name.replace('.', ''))
                        if not scraped_player:
                            continue
                    else:
                        continue

                yahoo_id = scraped_player['id'].split('.')[-1]
                yahoo_name = scraped_player['display_name']

            values = (yahoo_id, yahoo_name, nfl_name)
            curs.execute('''UPDATE player
                            SET yahoo_id = ?, yahoo_name = ?
                            WHERE nfl_name = ?''', values)
            conn.commit()


def update_stats_database():
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

    statlines_file = os.path.normpath('data/statlines.json')

    with open(statlines_file, 'r') as f:
        statlines = json.load(f)

    for stat, details in statlines.items():
        vals = (details['name'], stat, None, None, None)
        curs.execute('''INSERT INTO statline (nfl_name, nfl_id, yahoo_name, yahoo_id, points)
                        VALUES (?, ?, ?, ?, ?)''', vals)

    conn.commit()


def scrape_player(p_name):
    p_name = urllib.parse.quote(p_name)

    search_url = f'https://sports.yahoo.com/site/api/resource/searchassist;searchTerm={p_name}'
    h = requests.get(search_url)

    if h.status_code != 200:
        return {}

    hits = h.json()['items']

    if len(hits) > 1:
        print(f'WARNING: more than one player found via screen scrape for {p_name}')

    try:
        data = hits[0]['data']
    except IndexError:
        print(f'DUMP: status {h.status_code} content {h.content}')
        return {}

    data = data.replace('\\', '')
    data = data.replace('{', '')
    data = data.replace('}', '')

    kv_pairs = data.split(',')

    d = {}
    for kv in kv_pairs:
        k, v = kv.split(':', 1)
        d[k] = v

    return d


if __name__ == '__main__':
    config = load_config()
    # update_player_database()
    # update_stats_database()
    # calc_week_stats()
    # get_league()
    find_players_by_score_type('5')

