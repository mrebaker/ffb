"""
Functions for interacting with Yahoo and NFL fantasy football APIs.
"""

# Standard library imports
import json

# Third-party imports
import retry
import yaml
import yahoo_fantasy_api as yapi
from yahoo_oauth import OAuth2

CONFIG = yaml.safe_load('config.yml')


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


# @retry(PotentialRateLimitError, delay=5, backoff=4, max_delay=250)
def player(p_name):
    """
    Gets the Yahoo fantasy details for a particular name.
    :param p_name: The player's name
    :return: a dict containing details from the Yahoo fantasy API
    """
    if "\'" in p_name:
        return []

    lg = league()
    try:
        details = lg.player_details(p_name)
    except json.decoder.JSONDecodeError:
        print(f'Waiting for player {p_name}... ')
        raise PotentialRateLimitError
    return details


def league():
    oauth = authenticate()
    lg = yapi.Game(oauth, 'nfl').to_league(CONFIG['league_id'])
    return lg
