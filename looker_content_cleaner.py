import csv
from time import sleep
import os
import looker
import sys

LOOK_IDS_FILE = sys.argv[1]
BACK_UP_FILE = sys.argv[2]


base_url = """https://{looker_instance}:19999/api/3.0/""".format(looker_instance=os.popen('echo $LOOKER_INSTANCE').read().rstrip())

client_id = looker_instance=os.popen('echo $API_CLIENT').read().rstrip()
client_secret = looker_instance=os.popen('echo $API_SECRET').read().rstrip()

# instantiate Auth API
unauthenticated_client = looker.ApiClient(base_url)
unauthenticated_authApi = looker.ApiAuthApi(unauthenticated_client)

# authenticate client
token = unauthenticated_authApi.login(client_id=client_id, client_secret=client_secret)
client = looker.ApiClient(base_url, 'Authorization', 'token ' + token.access_token)

# instantiate User API client
userApi = looker.UserApi(client)
me = userApi.me();

# instantiate Look API client
lookApi = looker.LookApi(client)

with open(BACK_UP_FILE,'w') as csvfile:
  backupWriter = csv.writer(csvfile)
  with open(LOOK_IDS_FILE) as csvfile:
    csvfile.seek(0)
    reader = csv.DictReader(csvfile)
    backupWriter.writerow(['title','space','fields','filters','pivots','dynamic_fields'])
    for row in reader:
      lookId = row['Look ID']
      sleep(0.1)
      look = lookApi.look(lookId)
      dynamic_fields = look.query.dynamic_fields if isinstance(look.query.dynamic_fields,list) else []
      print """About to delete '{title}'""".format(title=look.title)
      backupWriter.writerow([look.title,look.space.name,look.query.fields,look.query.filters,look.query.pivots,''.join(dynamic_fields)])

      lookApi.delete_look(lookId)


