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
from pathlib import Path
import urllib.parse

# third party imports
import numpy as np
import pandas as pd
import plotly.express as px
import requests
import yaml
from matplotlib import pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

# local imports
import api
import db
import util

with open('_config.yml', 'r') as config_file:
    CONFIG = yaml.safe_load(config_file)


def box_plot(position, top_n):
    _, curs = db.connect()
    rows = curs.execute(
        '''SELECT player.nfl_name as player_name, season, week, points, t.scoring_rank
           FROM (player_weekly_points LEFT JOIN player on player_weekly_points.player_nfl_id = player.nfl_id)
           LEFT JOIN (SELECT player_nfl_id, RANK () OVER ( ORDER BY SUM(points) Desc ) scoring_rank
                      FROM player_weekly_points 
                      LEFT JOIN player on player_weekly_points.player_nfl_id=player.nfl_id
                      WHERE season = 2019 and player.eligible_positions = ?
                      GROUP BY player_nfl_id) as t 
                      on t.player_nfl_id = player_weekly_points.player_nfl_id
           WHERE player.eligible_positions = ? AND season = 2019
           GROUP BY player.nfl_name, season, week, points
           HAVING scoring_rank <= ?
           ORDER BY scoring_rank''', (position, position, top_n)
        ).fetchall()
    df = pd.DataFrame(rows)
    fig = px.box(df, x='player_name', y='points')
    fig.show()


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

    api_response = league.matchups(week)
    week_matchups = api_response['fantasy_content']['league'][1]['scoreboard']['0']['matchups']
    print(f"------ Week {week} ------")
    for val in week_matchups.values():
        if isinstance(val, int):
            continue
        team1 = val['matchup']['0']['teams']['0']['team'][0][2]['name']
        team2 = val['matchup']['0']['teams']['1']['team'][0][2]['name']
        team1_score = team_points[team1]
        team2_score = team_points[team2]
        print(f'{team1} {team1_score:.2f} v {team2_score:.2f} {team2}')

    for team, players in team_missing_players.items():
        if players:
            print(f'{team} missing {", ".join(players)}')

    for team, multipliers in team_missing_multipliers.items():
        if multipliers:
            print(f'{team} missing multipiers:', multipliers)


def consistency_chart(frequency):
    _, curs = db.connect()
    if frequency == 'season':
        result = curs.execute('''SELECT player.nfl_name as player_name, season, sum(points) as points
                                 FROM player_weekly_points 
                                 LEFT JOIN player on player_weekly_points.player_nfl_id = player.nfl_id
                                 WHERE player.eligible_positions = 'QB'
                                 GROUP BY player.nfl_name, season''').fetchall()
        x_data = 'season'
        x_axis_ticks = dict(tickmode='array', tickvals=[2015, 2016, 2017, 2018, 2019],
                            ticktext=['2015', '2016', '2017', '2018', '2019'])

    elif frequency == 'week':
        result = curs.execute('''SELECT player.nfl_name as player_name, season, week, 
                                 (season || "-" || week) as game, points
                                 FROM player_weekly_points 
                                 LEFT JOIN player on player_weekly_points.player_nfl_id = player.nfl_id
                                 WHERE player.eligible_positions = "QB"''').fetchall()
        x_data = 'game'
    else:
        raise ValueError('Frequency must be "season" or "week".')

    df = pd.DataFrame(result)
    fig = px.line(df, x=x_data, y='points', line_group='player_name', color='player_name')

    if frequency == 'season':
        fig.update_layout(xaxis=x_axis_ticks)
    else:
        fig.update_layout(xaxis_tickformat='%m<br>%Y')

    fig.show()


def correlate_years():
    _, curs = db.connect()
    result = curs.execute('''SELECT player.nfl_name as player_name, season, sum(points)
                             FROM player_weekly_points 
                             LEFT JOIN player on player_weekly_points.player_nfl_id = player.nfl_id
                             WHERE player.eligible_positions = 'WR'
                             GROUP BY player.nfl_name, season''').fetchall()

    df = pd.DataFrame(result)
    df = df.pivot(index='player_name', columns='season', values='sum(points)')
    df = df.reset_index()

    fig = px.scatter(df, x=2018, y=2019, text='player_name')
    fig.show()


def evaluate_predictions():
    """
    Gets player predictions for each available week and compares with predicted points.
    :return: nothing
    """
    week_limit = api.league().current_week()
    teams = api.league().teams()
    points_list = []
    for team in teams:
        team_obj = api.league().to_team(team['team_key'])
        for week in range(1, week_limit):
            d = {}
            matchup = team_obj.matchup(week)
            points = matchup[0]['0']['teams']['0']['team'][1]
            d['team_id'] = team['team_key']
            d['week'] = week
            d['proj_points'] = float(points['team_projected_points']['total'])
            d['act_points'] = float(points['team_points']['total'])

            points_list.append(d)

    df = pd.DataFrame(points_list)
    df['residual'] = df['act_points'] - df['proj_points']
    fig, axs = plt.subplots(nrows=1, ncols=2)

    axs[0].scatter(df['proj_points'], df['act_points'], c=df['residual'], cmap='plasma')
    axs[1].scatter(df['proj_points'], df['residual'])

    axs[0].set_xlabel = 'projected points'
    axs[0].set_ylabel = 'actual points'
    plt.show()


def find_players_by_score_type(nfl_score_id, period):
    """
    Prints a table of all players who recorded particular box score stats.
    :param nfl_score_id: The ID of the requested stat per the NFL Fantasy API
    :param period: "season" for the whole season, otherwise just uses 2019 week 10
    :return:
    """
    if period == 'season':
        score_file = Path('data_in/nfl-seasonstats-2019-10.json')
    else:
        score_file = Path('data_in/nfl-weekstats-2019-10.json')

    with open(score_file, 'r') as f:
        week_stats = json.load(f)

    filtered = [i for i in week_stats['players'] if nfl_score_id in i['stats'].keys()]

    for player in filtered:
        attr_to_show = [player['name'], player['teamAbbr'], player['position'], player['stats'][nfl_score_id]]

        print('\t'.join(attr_to_show))


def minmax(position):
    """
    Plots the best and worst weekly rankings for each player in the specified position group.
    :param position: str, 2 letters representing position group e.g. QB
    :return: Nothing
    """
    unused_conn, curs = db.connect()
    players = curs.execute('SELECT * FROM player WHERE eligible_positions = ?', (position,))

    df_players = pd.DataFrame(players)
    df_ranks = pd.concat([position_rankings(position, 2019, week, False) for week in range(1, 18)])

    df = df_players.merge(right=df_ranks, how='inner', on='nfl_id')
    df.to_csv('output.csv')
    df = df[['week', 'rank', 'nfl_name']]
    df = df.sort_values(['week', 'rank'])
    df = df.pivot(index='nfl_name', columns='week', values='rank').reset_index()
    weeks = [i for i in range(1, 18)]
    df['games_played'] = df[weeks].count(axis=1)
    df['worst'] = df[weeks].max(axis=1)
    df['best'] = df[weeks].min(axis=1)
    df['median'] = df[weeks].median(axis=1)

    fig2 = px.scatter_3d(df, x='best', y='worst', z='median', text='nfl_name', color='games_played')
    fig2.show()


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


def player_points_history(yahoo_id):
    _, curs = db.connect()
    rows = curs.execute('''SELECT p.season, p.week, p.points FROM player_weekly_points as p
                           LEFT JOIN player on p.player_nfl_id = player.nfl_id
                           WHERE player.yahoo_id = ?''', (yahoo_id,)).fetchall()

    df = pd.DataFrame(rows)

    season_start = df['season'].min()
    season_end = df['season'].max()
    for season in range(season_start, season_end + 1):
        for week in range(1, 18):
            # 17 weeks in a regular season
            df_game = df[(df['season'] == season) & (df['week'] == week)]
            if df_game.empty:
                df_temp = pd.DataFrame([[season, week, 0]], columns=df.columns)
                df = df.append(df_temp)
    df = df.sort_values(['season', 'week'])
    df['game'] = df['season'].map(str) + df['week'].map(str)

    plt.bar(x=df['game'], height=df['points'])
    locs, _ = plt.xticks()
    plt.xticks(locs, labels=df['week'])
    plt.show()


def player_weekly_rankings(*yahoo_ids, plot=True):
    """
    Gets the weekly ranking for a given player within their position group.
    :param yahoo_ids: any number of Yahoo ID(s) for player(s) to search
    :param plot: whether to show plots of the weekly rankings or not
    :return: a list of the weekly rankings for the player, from Week 1 to the previous week
    """

    league = api.league()
    unused_conn, curs = db.connect()

    query = f'SELECT * FROM player WHERE yahoo_id IN ({",".join("?" * len(yahoo_ids))})'
    players = curs.execute(query, yahoo_ids).fetchall()

    if not players:
        return []

    end_week = league.current_week()
    rankings = {}

    if plot:
        fig = plt.figure()
        ax = plt.subplot(111)

    for player in players:
        player_rankings = []
        for week in range(1, end_week):
            pos_rank = position_rankings(player['eligible_positions'], 'week', week)
            stat_row = pos_rank[pos_rank['yahoo_id'] == player['yahoo_id']]
            if stat_row.iloc[0]['DNS'] == 1:
                week_score = np.nan
            else:
                week_score = stat_row.index.values.astype(int)[0]
            player_rankings.append(week_score)

        rankings[player['yahoo_id']] = player_rankings
        if plot:
            ax.plot(player_rankings, label=player['yahoo_name'])

    if plot:
        box = ax.get_position()
        # shrink plot width by 20% to allow room for legend
        ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
        ax.legend(loc='center left', bbox_to_anchor=(1.01, 0.5))
        # flip Y axis so good weeks are on top
        fig.gca().invert_yaxis()
        plt.show()

    return rankings


def position_rankings(position, season, week, season_stats: bool):
    """
    Ranks all the players within a specified position for a specified week.
    :param position: 2-letter code representing a position e.g. QB
    :param season: int representing a season (i.e. year)
    :param week: int representing a week of the fantasy football season e.g. 9.
    :param season_stats: bool, True if you want full season ranking, False for a week
    :return: a sorted dataframe of all players in that position for that week
    """
    unused_conn, curs = db.connect()
    players = curs.execute("""SELECT nfl_id, yahoo_id, yahoo_name FROM player
                              WHERE eligible_positions LIKE ?""", (f'%{position}%',)).fetchall()

    stat_type = 'season' if season_stats else 'week'
    stats = util.load_stat_file(stat_type, season, week)

    for player in players:
        try:
            stat_lines = stats[player['nfl_id']]['stats']['week']['2019'][f'{week:02}']
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
    # don't include players who didn't start
    df = df[df['DNS'] == 0]
    df = df[['nfl_id', 'yahoo_id', 'yahoo_name', 'pts']]
    df = df.sort_values(by=['pts'], axis=0, ascending=False)
    # create ranking as the index
    df = df.reset_index(drop=True)
    df.index = range(1, len(df) + 1)

    # reset to preserve the ranking in a column
    df = df.reset_index(drop=False).rename(columns={'index': 'rank'})

    df['season'] = season
    df['week'] = week

    return df


def risk_reward(position, season):
    """
    Charts players within a position group by their whole-season rank vs variance in rank.
    :param position: string representing a position group e.g. 'QB'
    :param season: integer season e.g. 2019
    :return: Nothing
    """
    _, curs = db.connect()
    player_points = curs.execute('''SELECT player.nfl_id, player.yahoo_id, player.yahoo_name,
                                    player_weekly_points.points
                                    FROM player LEFT JOIN player_weekly_points 
                                    ON player.nfl_id=player_weekly_points.player_nfl_id 
                                    WHERE player.eligible_positions = ? 
                                    AND player_weekly_points.season = ?
                                    ''',
                                 (position, season))
    df = pd.DataFrame(player_points)
    df['points'] = df['points'].astype(float)
    # de minimis threshold
    df = df[df['points'] >= 5]
    df = df.groupby(['nfl_id', 'yahoo_id', 'yahoo_name'])['points'].agg([np.sum, np.var])
    df = df.reset_index()
    fig = px.scatter(df, x='sum', y='var', text='yahoo_name')
    fig.update_traces(textposition='top center')
    fig.show()


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
    score_file = Path(f'data_in/nfl-weekstats-2019-{week}.json')
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
                                     WHERE yahoo_id = ?''', (player['player_id'],)).fetchone()

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
    print(api.free_agents('QB'))
