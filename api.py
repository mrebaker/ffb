"""
Functions for interacting with Yahoo and NFL fantasy football APIs.
"""

# Standard library imports
import json
import logging
import urllib.parse
from pathlib import Path
import requests

# Third-party imports
import pandas as pd
import yaml
import yahoo_fantasy_api as yapi
from yahoo_oauth import OAuth2

with open('_config.yml', 'r') as config_file:
    CONFIG = yaml.safe_load(config_file)

log = logging.getLogger()
logging.basicConfig(filename='ffb.log', level=logging.DEBUG)


class PotentialRateLimitError(BaseException):
    """
    Custom exception to allow retry of failed API call.
    """


def authenticate():
    """
    Creates an authenticated Yahoo API session, getting a new token if necessary.
    :return: the authenticated session
    """
    auth = OAuth2(None, None, from_file='_oauth.json')
    if not auth.token_is_valid():
        auth.refresh_access_token()
    return auth


def free_agents(position=None):
    """
    Gets free agents at a given position, or for all positions if none specified.
    :param position: Optional string representing a position group e.g. QB
    :return: pandas data frame
    """
    lg = league()
    if position:
        df = pd.DataFrame(lg.free_agents(position))
    else:
        dfs = [pd.DataFrame(lg.free_agents(pos)) for pos in lg.positions()]
        df = pd.concat(dfs)

    return df


def player(p_name=None, p_id=None):
    """
    Gets the Yahoo fantasy details for a particular name.
    :param p_name: The player's name
    :param p_id: The player's Yahoo ID
    :return: a dict containing details from the Yahoo fantasy API
    """
    lg_obj = league()
    log = logging.getLogger()

    if p_id:
        log.info(f'Searching {p_id}')
        player_key = f'nfl.p{p_id}'
        url = 'http://fantasysports.yahooapis.com/fantasy/v2/player/' + player_key
        # TODO - authenticate oauth session
        ret = requests.get(url)
        print(ret)
        return

    if "\'" in p_name:
        return []

    try:
        log.info(f'Searching {p_name}')
        details = lg_obj.player_details(p_name)
    except json.decoder.JSONDecodeError:
        log.warning(f'Potential rate limit error for player {p_name}')
        details = []
        # raise PotentialRateLimitError
    return details


def players():
    lg = league()

    ret = []
    start_pos = 0
    while True:
        api_response = lg.yhandler.get_players_raw(lg.league_id, start_pos*25)

        if api_response is None or start_pos > 100:
            break

        player_set = api_response['fantasy_content']['league'][1]['players']
        if not player_set:
            break

        for player_dict in player_set.items():
            try:
                player_details = player_dict[1]['player'][0]
            except TypeError:
                continue
            clean_dict = {}
            for detail in player_details:
                try:
                    for k, v in detail.items():
                        clean_dict[k] = v
                except AttributeError:
                    pass
            ret.append(clean_dict)

        start_pos += 1

    return ret


def league():
    """
    Returns a league from the Yahoo API based on the config file.
    :return: dict representing the league
    """
    oauth = authenticate()
    lg_obj = yapi.Game(oauth, 'nfl').to_league(CONFIG['league_id'])
    return lg_obj


def download_game_data():
    for year in range(2015, 2020):
        for week in range(1, 18):
            file_path = Path(f'data_in/nfl-weekstats-{year}-{week:02}.json')
            if not file_path.exists():
                url = f'https://api.fantasy.nfl.com/v2/players/weekstats?season={year}&week={week:02}'
                response = requests.get(url)
                if response.status_code == 200:
                    with open(file_path, 'w+') as f:
                        f.write(response.text)


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

    filtered_hits = [hit for hit in hits if hit['data']['league'] == 'NFL']

    if not filtered_hits:
        return {}

    return filtered_hits[0]['data']
