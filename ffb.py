import yaml
import yahoo_fantasy_api as yapi
from yahoo_oauth import OAuth2


def authenticate():
    # auth = OAuth2(config['client_id'], config['client_secret'])
    auth = OAuth2(None, None, from_file='oauth.json')
    if not auth.token_is_valid():
        auth.refresh_access_token()
    return auth


def load_config():
    with open('config.yml', 'r') as f:
        conf = yaml.safe_load(f)
    return conf


if __name__ == '__main__':
    config = load_config()
    oauth = authenticate()
    game = yapi.Game(oauth, 'nfl')
    league = game.to_league(config['league_id'])

    print(league.teams())
