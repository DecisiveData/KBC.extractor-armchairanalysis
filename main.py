import csv
import datetime
import json
import os
import requests
import time
from requests.auth import HTTPBasicAuth

#Keboola
DEFAULT_FILE_INPUT       = "/data/in/tables/"
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

skip_game = []

#Endpoints
ept = dict()

## Schedule & Games
ept['schedule']         = '/schedule'   # All Games
ept['games']            = '/games'      # Games by Season
                                        # Games by Team
ept['game']             = '/game'       # Game Detail
ept['snaps']            = '/snaps/'      # Snaps by Game

## Drives & Plays
ept['drives']           = '/drives/'    # Drives by Game
                                        # Drives by Team
ept['plays']            = '/plays/'     # Plays by Game
ept['play']             = '/play/'       # Play Detail

## Players
ept['players']          = '/players'    # All Players
                                        # All Players (active)
                                        # Players (active) by Team
ept['player']           = '/player/'    # Player Detail
                                        # Player Detail by Name
ept['player-college']   = '/college'    # Player College Detail
ept['player-defense']   = '/defense'    # Player Defense Detail
ept['player-offense']   = '/offense'    # Player Defense Detail
ept['player-tweets']    = '/tweets'     # Tweets by Player

def get_endpoint_json(endpoint, authorization=AUTH, parameters=dict()):
    #It appears that you get to make 120 API calls. You must not call the API for 60 seconds in a row in order to get that limit reset.
    r = requests.get(endpoint, params=parameters, auth=authorization)
    limit = r.headers.get('X-RateLimit-Limit')
    remaining = r.headers.get('X-RateLimit-Remaining')
    retry_after = r.headers.get('Retry-After')
    if remaining is not None and limit is not None:
        print('Requests: ' + remaining + '/' + limit + ' : ' + endpoint)
    if not (retry_after is None) or r.status_code == 429: #Too Many Requests
        print('Pausing for ' + retry_after + ' seconds at UTC: ' + datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") + '.')
        time.sleep(int(retry_after) + 1)
        r = requests.get(endpoint, params=parameters, auth=authorization)
    if r.status_code != 200:
        print('Unsuccessful: Status Code ' + str(r.status_code) + r.reason + ' at ' + endpoint)
        print(json.dumps(r.json()))
        if r.status_code == 404 and endpoint.startswith(cfg['url'] + ept['drives']):
            last_slash = endpoint.rfind('/') + 1
            length = len(endpoint)
            right = (length-last_slash) * -1
            gid = endpoint[right:]
            if int(gid) not in skip_game:
                skip_game.append(int(gid))
        return None
    else:
        return r.json()

def iterate_endpoint_json(endpoint, authorization=AUTH, start=cfg['start_default'], count=cfg['count_default'], parameters=dict()):
    parameters['start'] = str(start)
    parameters['count'] = str(count)

    doc_list = []
    while start > 0:
        doc = get_endpoint_json(endpoint, authorization, parameters)
        doc_list.extend(doc['data'])
        if len(doc['data']) < count:
            start = -1
        else:
            start += count
            parameters['start'] = str(start)
    return doc_list

def populate_season(game_list):
    season_list = []
    for game in game_list:
        if game['seas'] not in season_list:
            season_list.append(game['seas'])
    return season_list

def populate_drive(game_list):
    drive_list = []
    for game in game_list:
        drive = get_endpoint_json(cfg['url'] + ept['drives'] + str(game['gid']))
        if drive is not None:
            drive_list.extend(drive['data'])
    return drive_list

def populate_play(game_list):
    play_list = []
    for game in game_list:
        play = get_endpoint_json(cfg['url'] + ept['plays'] + str(game['gid']), parameters={'mode':'expanded'})
        if play is not None:
            play_list.extend(play['data'])
    return play_list

def populate_snap(game_list):
    snap_list = []
    for game in game_list:
        snap = get_endpoint_json(cfg['url'] + ept['snaps'] + str(game['gid']))
        if snap is not None:
            snap_list.extend(snap['data'])
    return snap_list

def populate_player_stat(player_list, stat, iterate=False):
    stat_list = []
    for player in player_list:
        endpoint = cfg['url'] + ept['player'] + str(player['player']) + ept['player-' + stat]
        if iterate is False:
            stat_doc = get_endpoint_json(endpoint)
        else:
            stat_doc = iterate_endpoint_json(endpoint)
        if stat_doc is not None:
            stat_list.extend(stat_doc['data'])
    return stat_list

def list_to_listdict(list, key):
    for i in range(0,len(list)):
        d = {}
        d[key] = list[i]
        list[i] = d
    return list

def pare_game_list(game_list_full):
    game_list = []
    for game in game_list_full:
        if game['gid'] not in skip_game:
            game_list.append(game)
    return game_list

###################
# universe of data to get
team_list           = list_to_listdict( ['ARI','ATL','BAL','BUF','CAR','CHI','CIN','CLE',
                                         'DAL','DEN','DET','GB','HOU','IND','JAC','KC',
                                         'LAC','LA','MIA','MIN','NE','NO','NYG','NYJ',
                                         'OAK','PHI','PIT','SF','SEA','TB','TEN','WAS']  #check for STL and SD, maybe oilers?
                                       , 'team')
#game_list_full      = get_endpoint_json(cfg['url'] + ept['schedule'])['data']
#season_list         = list_to_listdict(populate_season(game_list_full),'season')
#drive_list          = populate_drive(game_list_full)

#game_list           = pare_game_list(game_list_full) #remove games we don't have access to

#play_list           = populate_play(game_list)
#snap_list           = populate_snap(game_list)
player_list         = iterate_endpoint_json(cfg['url'] + ept['players'])
#player_active_list  = iterate_endpoint_json(cfg['url'] + ept['players'], parameters= {'status': 'active'})

#player_college_list = populate_player_stat(player_list, 'college')
#player_defense_list = populate_player_stat(player_list, 'defense')
#player_offense_list = populate_player_stat(player_list, 'offense')
#player_tweet_list   = populate_player_stat(player_list, 'tweets')

##################
# write files
def write_file(list, filename):
    with open(filename, 'wt') as out_file:
        writer = csv.DictWriter(out_file, fieldnames=['data','time','file'], lineterminator='\n', quoting=csv.QUOTE_ALL)
        for item in list:
            writer.writerow({ 'data': json.dumps(item), 'time': cfg['now'], 'file': filename})
    return

#write_file(team_list, 'team.csv')
#write_file(game_list, 'game.csv')
#write_file(season_list, 'season.csv')
#write_file(drive_list, 'drive.csv')
#write_file(play_list, 'play.csv')
#write_file(snap_list, 'snap.csv')
#write_file(player_list, 'player.csv')
#write_file(player_active_list, 'player_active.csv')

#write_file(player_college_list, 'player_college.csv')
#write_file(player_defense_list, 'player_defense.csv')
#write_file(player_offense_list, 'player_offense.csv')
#write_file(player_tweet_list, 'player_tweets.csv')

print('Done')