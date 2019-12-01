"""
Utilities to support the fantasy football analysis modules e.g. file I/O
"""

# standard library imports
import json
import os
import requests

# local imports
import api


def download_stat_file(stat_type, week):
    """
    Gets the player stats for the given season and week
    :param stat_type: str 'week' or 'season'
    :param week: the week requested, or None for the last completed
    :return: nothing
    """

    if stat_type == 'week':
        url = f'https://api.fantasy.nfl.com/v2/players/weekstats?season=2019&week={week}'
    elif stat_type == 'season':
        url = f'http://api.fantasy.nfl.com/v1/players/stats?statType=seasonStats&season=2019&week={week}&format=json'
    else:
        raise RuntimeError("stat_type must be 'week' or 'season'")

    resp = requests.get(url)

    if resp.status_code == 200:
        filename = f'data/nfl-{stat_type}stats-2019-{week:02}.json'
        with open(filename, 'w+') as f:
            f.write(resp.text)
    else:
        raise requests.HTTPError


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
        download_stat_file(stat_type, week)
        with open(score_file, 'r') as f:
            stats = json.load(f)['games']['102019']['players']

    return stats
