"""
ffb

Uses data from APIs including nfl.com and Yahoo Fantasy Football to help a fantasy football manager.
A lot of work left to do, but aims are to:
 - identify waiver pickups
 - learn from past mistakes (cut players getting better, acquired players declining etc
 - make better use of waiver budget
"""

# standard library imports
import json
import os
import sqlite3
import urllib.parse
from datetime import datetime as dt

import numpy as np
import pandas as pd
import requests
import yaml
# third party imports
from matplotlib import pyplot as plt

# local imports
import api
import db
import util

with open('config.yml', 'r') as config_file:
    CONFIG = yaml.safe_load(config_file)


def calc_week_stats(week=None):
    """
    Outputs the scores for each matchup in the given week, or the current week if not provided.
    :param week: Integer referring to a week of the fantasy season
    :return: Nothing
    """
    league = api.league()

    week = week or league.current_week()

    team_points = {}
    team_missing_players = {}
    team_missing_multipliers = {}

    for team in league.teams():
        score, missing_players = team_weekly_score(team, week, league)
        points, missing_multipliers = points_from_scores(score)
        team_points[team['name']] = points
        team_missing_multipliers[team['name']] = missing_multipliers
        team_missing_players[team['name']] = missing_players

    week_matchups = league.matchups(week)

    print(f"------ Week {week} ------")
    for val in week_matchups.values():
        team1 = val['0']['team'][0][2]['name']
        team2 = val['1']['team'][0][2]['name']
        team1_score = team_points[team1]
        team2_score = team_points[team2]
        print(f'{team1} {team1_score:.2f} v {team2_score:.2f} {team2}')

    for team, players in team_missing_players.items():
        if players:
            print(f'{team} missing {", ".join(players)}')

    for team, multipliers in team_missing_multipliers.items():
        if multipliers:
            print(f'{team} missing multipiers:', multipliers)


def find_players_by_score_type(nfl_score_id, period):
    """
    Prints a table of all players who recorded particular box score stats.
    :param nfl_score_id: The ID of the requested stat per the NFL Fantasy API
    :param period: "season" for the whole season, otherwise just uses 2019 week 10
    :return:
    """
    if period == 'season':
        score_file = os.path.normpath('data/nfl-seasonstats-2019-10.json')
    else:
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


def log(msg):
    """
    Very basic log file writer
    :param msg: the text to write to the log file
    :return: Nothing
    """
    log_file = 'log.txt'
    with open(log_file, 'a+') as f:
        f.write(f'{dt.now().strftime("%Y-%m-%d %H:%M:s")} {msg}\n')


def update_player_database():
    """
    Adds players from a week stat file, if missing from the database.
    :return:
    """
    conn, curs = db.connect()

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

    players = curs.execute('''SELECT id, nfl_name, yahoo_name,
                              yahoo_id, eligible_positions FROM player''').fetchall()

    for player in players:
        # add Yahoo details if missing
        if player['yahoo_id'] is None or player['yahoo_name'] is None:
            player_yahoo_profile = api.player(player['nfl_name'])
            try:
                yahoo_id = player_yahoo_profile['player_id']
                yahoo_name = player['nfl_name']
            except TypeError:
                log(f'Unable to match NFL player name "{player["nfl_name"]}" to a Yahoo player.')
                log('Trying screen scrape')
                scraped_player = scrape_player(player['nfl_name'])

                if not scraped_player:
                    if '.' in player['nfl_name']:
                        scraped_player = scrape_player(player['nfl_name'].replace('.', ''))
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
            player_yahoo_profile = api.player(player['yahoo_name'])
            try:
                eligible_positions = player_yahoo_profile['eligible_positions']
                # Yahoo API returns a list of dicts, so extract the dict values
                position_list = [d['position'] for d in eligible_positions]
                position_text = ",".join(position_list)
            except TypeError:
                scraped_player = scrape_player(player['nfl_name'])
                if not scraped_player:
                    if '.' in player['nfl_name']:
                        scraped_player = scrape_player(player['nfl_name'].replace('.', ''))
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


def points_from_scores(score_dict):
    """
    Calculates the points total for a set of scores, based on the multipliers in the database.
    :param score_dict: a dict containing the scores and their volume
    :return: points total and a dict of score IDs that have no multiplier in the database.
    """
    unused_conn, curs = db.connect()
    stat_modifiers = curs.execute('SELECT * FROM statline').fetchall()

    missing_multipliers = {}
    points = 0
    for stat, value in score_dict.items():
        try:
            multiplier = next(i['points'] for i in stat_modifiers if i['nfl_id'] == stat)
        except StopIteration:
            print(f'No stat with NFL ID {stat} in the database. Defaulting to 0 points.')
            multiplier = 0

        if multiplier is None:
            try:
                missing_multipliers[str(stat)] += value
            except KeyError:
                missing_multipliers[str(stat)] = value
            multiplier = 0

        points += value * multiplier

    return points, missing_multipliers


def player_weekly_rankings(*yahoo_ids):
    """
    Gets the weekly ranking for a given player within their position group.
    :param yahoo_ids: any number of Yahoo ID(s) for player(s) to search
    :return: a list of the weekly rankings for the player, from Week 1 to the previous week
    """

    league = api.league()
    unused_conn, curs = db.connect()

    query = f'SELECT * FROM player WHERE yahoo_id IN ({",".join("?"*len(yahoo_ids))})'
    players = curs.execute(query, yahoo_ids).fetchall()

    if not players:
        return []

    end_week = league.current_week()
    rankings = {}

    fig = plt.figure()
    ax = plt.subplot(111)

    for player in players:
        player_rankings = []
        for week in range(1, end_week):
            pos_rank = position_rankings(player['eligible_positions'], week)
            stat_row = pos_rank[pos_rank['yahoo_id'] == player['yahoo_id']]
            if stat_row.iloc[0]['DNS'] == 1:
                week_score = np.nan
            else:
                week_score = stat_row.index.values.astype(int)[0]
            player_rankings.append(week_score)

        rankings[player['yahoo_id']] = player_rankings
        ax.plot(player_rankings, label=player['yahoo_name'])

    box = ax.get_position()

    # shrink plot width by 20% to allow room for legend
    ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])

    ax.legend(loc='center left', bbox_to_anchor=(1.01, 0.5))

    # flip Y axis so good weeks are on top
    fig.gca().invert_yaxis()
    plt.show()
    return rankings


def position_rankings(position, stat_type, period=None):
    """
    Ranks all the players within a specified position for a specified week.
    :param position: 2-letter code representing a position e.g. QB
    :param stat_type: str 'season ' or 'week'
    :param period: int representing a week of the fantasy football season e.g. 9 or none for
                   current week
    :return: a sorted dataframe of all players in that position for that week
    """
    unused_conn, curs = db.connect()
    players = curs.execute("""SELECT nfl_id, yahoo_id, yahoo_name FROM player
                              WHERE eligible_positions LIKE ?""", (f'%{position}%',)).fetchall()

    stats = util.load_stat_file(stat_type, period)
    period = period or api.league().current_week()

    for player in players:
        try:
            stat_lines = stats[player['nfl_id']]['stats']['week']['2019'][f'{period:02}']
        except KeyError:
            player['DNS'] = 1
            continue
        for stat_id, volume in stat_lines.items():
            stat_name = curs.execute("""SELECT nfl_name FROM statline
                                        WHERE nfl_id = ?""", (stat_id,)).fetchone()['nfl_name']
            if volume is None:
                player[stat_name] = 0
            else:
                player[stat_name] = float(volume)

    df = pd.DataFrame(players)
    df = df.fillna(0)
    df = df.sort_values(by=['pts'], axis=0, ascending=False)
    df = df.reset_index(drop=True)
    df.index = range(1, len(df)+1)
    df.to_csv(f'position_rankings_{period:02}_{position}.csv')
    return df


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

    nfl_stats_file = os.path.normpath('data/nfl-stats.json')

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


def scrape_player(p_name):
    """
    If searching for a player in the Yahoo API fails, try to scrape their details from the website.
    Why aren't all players in the API? Good question.
    :param p_name: player name
    :return: dict representing the information provided via the Yahoo website.
    """
    p_name = urllib.parse.quote(p_name)

    search_url = f'https://sports.yahoo.com/site/api/resource/searchassist;searchTerm={p_name}'
    response = requests.get(search_url)

    if response.status_code != 200:
        return {}

    hits = response.json()['items']

    if not hits:
        return {}

    for hit in hits:
        json_str = hit['data'].replace('\\', '"')
        hit['data'] = json.loads(json_str)

    if len(hits) > 1:
        filtered_hits = []
        for hit in hits:
            if hit['data']['league'] == 'NFL':
                filtered_hits.append(hit)
        print(f'WARNING: {len(hits)} players found via screen scrape for {p_name}.',
              f'After filtering by league, {len(filtered_hits)} player(s) remain.')

        if filtered_hits:
            hits = filtered_hits
        else:
            return {}

    return hits[0]['data']


def team_weekly_score(team, week, league):
    """
    Gets all the scores accrued by a fantasy team for a given week of the league season.
    :param team: dict representing the team resource from Yahoo API
    :param week: int for the chosen fantasy week
    :param league: object representing the league resource from Yahoo API
    :return: dict of scores accrued, and a dict of players not in database or stat file
    """
    unused_conn, curs = db.connect()
    score_file = os.path.normpath(f'data/nfl-weekstats-2019-{week}.json')
    with open(score_file, 'r') as f:
        week_stats = json.load(f)

    player_stats = week_stats['games']['102019']['players']
    roster = league.to_team(team['team_key']).roster(week=week)

    scores = {}
    missing_players = []

    for player in roster:
        if player['selected_position'] in ['BN', 'IR']:
            continue

        qry_result = curs.execute('''SELECT nfl_id
                                     FROM player 
                                     WHERE yahoo_id = ?''',
                                  (player['player_id'],)).fetchone()

        if qry_result:
            nfl_id = qry_result['nfl_id']
        else:
            txt = f'{player["name"]} (not in database using yahoo_id {player["player_id"]})'
            missing_players.append(txt)
            continue

        try:
            stats = player_stats[f'{nfl_id}']
        except KeyError:
            missing_players.append(f'{player["name"]} (not in stats using nfl_id {nfl_id})')
            continue

        for k, v in stats['stats']['week']['2019'][f'{week:02}'].items():
            if k == 'pts':
                continue
            if k not in scores.keys():
                scores[k] = int(v)
            else:
                scores[k] += int(v)

    return scores, missing_players


if __name__ == '__main__':
    # update_player_database()
    # update_stats_database()
    # print(ffb_api.league())

    # for w in range(1, 2):
    #     calc_week_stats(w)

    print(position_rankings('QB', 'week', 8))
    print(position_rankings('QB', 'season'))
    # print(player_weekly_rankings('30125'))
