"""
Functions for interacting with Yahoo and NFL fantasy football APIs.
"""

# Standard library imports
import json
import urllib.parse
import requests

# Third-party imports
import yaml
import yahoo_fantasy_api as yapi
from yahoo_oauth import OAuth2

with open('config.yml', 'r') as config_file:
    CONFIG = yaml.safe_load(config_file)


class PotentialRateLimitError(BaseException):
    """
    Custom exception to allow retry of failed API call.
    """


def authenticate():
    """
    Creates an authenticated Yahoo API session, getting a new token if necessary.
    :return: the authenticated session
    """
    auth = OAuth2(None, None, from_file='oauth.json')
    if not auth.token_is_valid(print_log=False):
        auth.refresh_access_token()
    return auth


def player(p_name):
    """
    Gets the Yahoo fantasy details for a particular name.
    :param p_name: The player's name
    :return: a dict containing details from the Yahoo fantasy API
    """
    if "\'" in p_name:
        return []

    lg_obj = league()
    try:
        details = lg_obj.player_details(p_name)
    except json.decoder.JSONDecodeError:
        print(f'Waiting for player {p_name}... ')
        raise PotentialRateLimitError
    return details


def league():
    """
    Returns a league from the Yahoo API based on the config file.
    :return: dict representing the league
    """
    oauth = authenticate()
    lg_obj = yapi.Game(oauth, 'nfl').to_league(CONFIG['league_id'])
    return lg_obj


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