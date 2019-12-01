"""
Utilities to support the fantasy football analysis modules e.g. file I/O
"""

# standard library imports
import json
import os
import requests

# local imports
import api


def download_weekstats(season, week):
    """
    Gets the player stats for the given season and week
    :param season: integer referring to the requested fantasy year e.g. 2019
    :param week: integer referring to the requested fantasy week
    :return: nothing
    """
    url = f'https://api.fantasy.nfl.com/v2/players/weekstats?season={season}&week={week:02}'
    resp = requests.get(url)

    if resp.status_code == 200:
        filename = f'data/nfl-weekstats-{season}-{week}.json'
        with open(filename, 'w+') as f:
            f.write(resp.text)


def load_stat_file(stat_type, week=None):
    """
    Loads a requested stat file, or downloads it if not yet saved. Raises an exception if the
    requested period has not yet started.
    :param stat_type: str 'week' or 'season'
    :param week: the week requested, or None for the last completed
    :return: a dict representing the file contents
    """

    if week is None:
        week = api.league().current_week() - 1

    score_file = os.path.normpath(f'data/nfl-{stat_type}stats-2019-{week:02}.json')

    try:
        with open(score_file, 'r') as f:
            stats = json.load(f)['games']['102019']['players']
    except FileNotFoundError:
        download_weekstats(2019, week)
        with open(score_file, 'r') as f:
            stats = json.load(f)['games']['102019']['players']

    return stats
