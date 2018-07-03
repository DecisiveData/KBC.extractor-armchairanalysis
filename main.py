import csv
import datetime
import json
import os
import requests
from requests.auth import HTTPBasicAuth

#test comment

#Keboola
DEFAULT_FILE_DESTINATION = "/data/out/tables/"

#Credentials
AUTH = None

cfg = dict()
cfg['user']     = None #''
cfg['password'] = None #''

if cfg['user'] is not None and cfg['password'] is not None:
  AUTH = HTTPBasicAuth(cfg['user'], cfg['password'])

#API basics
cfg['base_url']         = 'https://armchairanalysis.com/api'
cfg['test_env']         = '/test'
cfg['version']          = '/1.0'
cfg['url_prod']         = cfg['base_url'] + cfg['version']
cfg['url_test']         = cfg['base_url'] + cfg['version'] + cfg['test_env']
cfg['url']              = cfg['url_test']
cfg['count_default']    = 1000
cfg['start_default']    = 1
cfg['now']              = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")+" +0000"

if AUTH is not None:
  cfg['url'] = cfg['url_prod']

#Endpoints
ept = dict()

## Schedule & Games
ept['schedule']         = '/schedule'   # All Games
ept['games']            = '/games'      # Games by Season
                                        # Games by Team
ept['game']             = '/game'       # Game Detail
ept['snaps']            = '/snaps'      # Snaps by Game

## Drives & Plays
ept['drives']           = '/drives'     # Drives by Game
                                        # Drives by Team
ept['plays']            = '/plays'      # Plays by Game
ept['play']             = '/play'       # Play Detail

## Players
ept['players']          = '/players'    # All Players
                                        # All Players (active)
                                        # Players (active) by Team
ept['player']           = '/player'     # Player Detail
                                        # Player Detail by Name
ept['tweets']           = '/tweets'     # Tweets by Player

def get_endpoint_json(endpoint, authorization=AUTH, parameters=dict()):
    try:
        r = requests.get(endpoint, params=parameters, auth=authorization)
    except Exception:
        print('Request error')
        return None
    else:
        if r.status_code != 200:
            print('Did not receive response code 200')
            return None
        else:
            doc = r.json()
            return doc

def iterate_endpoint_json(endpoint, authorization=AUTH, start=cfg['start_default'], count=cfg['count_default'], parameters=dict()):
    parameters['start'] = str(start)
    parameters['count'] = str(count)

    doc_list = []
    while start > 0:
        doc = get_endpoint_json(endpoint, authorization, parameters)
        doc_list.append(doc)
        if len(doc['data']) < count:
            start = -1
        else:
            start += count
            parameters['start'] = str(start)
    return doc_list

###################
# universe of data to get
team_list           = ['ARI','ATL','BAL','BUF','CAR','CHI','CIN','CLE',
                       'DAL','DEN','DET','GB','HOU','IND','JAC','KC',
                       'LAC','LA','MIA','MIN','NE','NO','NYG','NYJ',
                       'OAK','PHI','PIT','SF','SEA','TB','TEN','WAS'] #check for STL and SD, maybe oilers?
season_list         = []
game_list           = []
drive_list          = []
play_list           = []
player_list         = []
player_active_list  = []

## season_list
## game_list
doc = get_endpoint_json(cfg['url'] + ept['schedule'])

for game_dict in doc['data']:
  if game_dict['seas'] not in season_list:
    season_list.append(game_dict['seas'])
  if game_dict['gid'] not in game_list:
    game_list.append(game_dict['gid'])

## player_list (iterative)
doc_list = iterate_endpoint_json(cfg['url'] + ept['players'])

for doc in doc_list:
    for player in doc['data']:
        if player['player'] not in player_list:
            player_list.append(player['player'])

## player_active_list (iterative)
doc_list = iterate_endpoint_json(cfg['url'] + ept['players'], parameters= {'status': 'active'})

for doc in doc_list:
    for player in doc['data']:
        if player['player'] not in player_active_list:
            player_active_list.append(player['player'])

##################
# files to write:
## teams.csv
## seasons.csv
## games.csv
## players.csv
## players_active.csv

def write_file(list, filename, endpoint, parameters=dict(), ts=cfg['now'], authorization=AUTH):
    for item in list:
      r = requests.get(endpoint, params=parameters, auth=authorization)

      if r.status_code == 200:
        doc = r.json()
        with open(filename, 'wt') as out_file:
      
          writer = csv.DictWriter(out_file, fieldnames=['data','time','file'], lineterminator='\n', quoting=csv.QUOTE_ALL)
          writer.writerow({ 'data': json.dumps(doc), 'time': ts, 'file': filename})
    return


write_file(season_list, 'games.csv', cfg['url'] + ept['schedule'])
write_file(player_list, 'players.csv', cfg['url'] + ept['players'])
#write_file(season_list, 'players_active.csv', cfg['url'] + ept['players'], payload)

pass