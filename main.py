import datetime
import json
import os
import requests
from requests.auth import HTTPBasicAuth

#Keboola
DEFAULT_FILE_DESTINATION = "/data/out/tables/"

#Credentials
AUTH = None

cfg = dict()
cfg['user']     = None
cfg['password'] = None

#API basics
cfg['base_url']         = 'https://armchairanalysis.com/api'
cfg['environment']      = '/test'
cfg['version']          = '/1.0'
cfg['url']              = cfg['base_url'] + cfg['version'] + cfg['environment']
cfg['count_default']    = 1000

#Endpoints
ept = dict()
ept['schedule']         = '/schedule'

if cfg['user'] is not None and cfg['password'] is not None:
  AUTH = HTTPBasicAuth(cfg['user'], cfg['password'])
else:
  pass

if AUTH is not None:
    r = requests.get(cfg['url'] + ept['schedule'], AUTH)
else:
    r = requests.get(cfg['url'] + ept['schedule'])

if r.status_code == 200:
  doc = r.json()

  with open('doc.json', 'w') as out_file:
      json.dump(doc, out_file)
pass