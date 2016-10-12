#!/usr/bin/env python
from __future__ import division
import re
import sys
import glob
import os
import math
from google.cloud import bigquery
from oauth2client.client import GoogleCredentials

credentials = GoogleCredentials.get_application_default()

bq_client = bigquery.Client(project='realself-main')
# (oauth_credentials=credentials,project='realself-main')
dataset = bq_client.dataset('rsdw_support')
assert dataset.exists()

days_ago = 0

def load(query,tableId):

    query_job = client.run_async_query(str(uuid.uuid4()), query)
    query_job.destination_table = {
      datasetId: 'rsdw_support'
    }
    query_job.begin()
    wait_for_job(query_job)

      job = {
          'projectId': 'realself-main'
          'configuration': {
              'query': {
                  'query': query,
                  'useLegacySql': True,
                  'destinationTable': {
                      'projectId': "realself-main",
                      'datasetId': "rsdw_support",
                      'tableId': tableId,
                  },
                  'allowLargeResults': True,
                  'flatten': False,
                  'createDisposition': 'CREATE_IF_NEEDED',
                  'writeDisposition': 'WRITE_TRUNCATE',
              }
          }
      }

def loadDaily():
    views = {
      'view_daily_user_snapshots': {
          'destination_tableId': 'daily_user_snapshots_'
        }
    }
    for view_name, value in views.iteritems():
      value.destination_tableId
      query = """
      # __ETL__ {table_name}
      SELECT
        DATE(DATE_ADD(TIMESTAMP(CONCAT(DATE( DATE_ADD( CURRENT_TIMESTAMP(), -7, 'HOUR')), ' 00:00:00')), -{days_ago}, 'DAY')) AS snapshot_date
        , *
      FROM [realself-main:rsdw_support.{view_name}]
      """.format(view_name=view_name, table_name=value.destination_tableId,days_ago=days_ago)



def main():
  #set inputs
  verbose = False
  if len(sys.argv) >= 2:
    VIEW_FOLDER = sys.argv[1]
    if VIEW_FOLDER.find('/',len(VIEW_FOLDER)-1) == -1:
      VIEW_FOLDER = VIEW_FOLDER+'/'
    days_ago = sys.argv[2]
  else:
    print("./bigquery_view_persist.py path_to_views days_ago")
    sys.exit()

  current_view = ''

  ordered_views = []

  def dateRange(matches):
    print('replaced Data Range in '+current_view)
    return """ (TABLE_DATE_RANGE(
  {table}
  ,TIMESTAMP(
    DATE_ADD(TIMESTAMP(CONCAT(DATE( DATE_ADD( CURRENT_TIMESTAMP(), -7, 'HOUR')), ' 00:00:00')), -{days}, 'DAY'))
  ,TIMESTAMP(
    DATE_ADD(
      DATE_ADD(
        DATE_ADD(
          TIMESTAMP(CONCAT(DATE( DATE_ADD( CURRENT_TIMESTAMP(), -7, 'HOUR')), ' 00:00:00'))
          , -{days}, 'DAY')
        , 1, 'DAY')
      ,-1, 'SECOND')
     )
  )
  )""".format(days=days_ago, table=matches.group(1))

  for filename in glob.glob(os.path.join(VIEW_FOLDER, '[a-z_]*.sql')):
    current_view = re.search('.*\/([a-z_]+).sql',filename).group(1)

    with open(filename) as f:
        view_query = f.read()
        new_query = re.sub('{% table_date_range +[^ ]+ +([^ %}]+) %}',
          dateRange, view_query)
        # if no change has been made, then we don't care about the order of this view
        if (len(view_query) == len(new_query)):
          ordered_views.append(current_view)
        else:
          ordered_views.insert(0,current_view)

        temp_file = filename+'.tmp'
        with open(temp_file,'w') as f:
          f.write(new_query)
          # with open(filename) as f:
          #   view_query = f.read()

  for view in ordered_views:
    filename = os.path.join(VIEW_FOLDER, view+'.sql.tmp')
    with open(filename) as f:
        view_query = f.read()
        table = dataset.table(name=view)
        table.view_query = view_query
        if (not table.exists()):
          print('creating a new view:'+view)
          table.create()
          assert table.exists()
        else:
          table.patch(
            view_query=view_query
          )
          assert table.view_query == view_query

  # CLEAN
  for filename in glob.glob(os.path.join(VIEW_FOLDER, '[a-z_]*.sql.tmp')):
    os.remove(filename)

  # materialize tables
  loadDaily()



if __name__ == '__main__':
  main()
