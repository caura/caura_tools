#!/usr/bin/env python
from __future__ import division
import re
import sys
import glob
import os
import math
import json
import uuid
from datetime import datetime, timedelta
import google
from google.cloud import bigquery, datastore
gbigquery = bigquery
from oauth2client.client import GoogleCredentials
import luigi
from luigi.contrib import bigquery

from bigquery import get_client

current_view = ''
dependencies = {}
is_overwrite = None
tables_to_delete = []

credentials = GoogleCredentials.get_application_default()

bq_client = gbigquery.Client(project='<project>')

dataset = bq_client.dataset('<dataset>')

assert dataset.exists()

days_ago = 0
hour_offset = 8

done_tables = {}
VIEW_FOLDER = ''
skipped_tables_count = 0

# TODO: CHANGE THIS FOR THE TABLES THAT NEED TO BE CHANGED
key_views = {
  '<view_name>': 1 # orders_pdt
}

date_dependent_views = {}

class dailyBuilds(bigquery.BigqueryLoadTask):
    credentials = GoogleCredentials.get_application_default()

    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
    query_mode = bigquery.QueryMode.INTERACTIVE

    def output(self,tableId):
        return bigquery.BigqueryTarget("<project>", "<dataset>", tableId, bigquery.BigqueryClient(oauth_credentials=self.credentials))

    def saveTable(self,query,tableId):
        print('saveTable:'+tableId)
        assert isinstance(self.output(tableId), bigquery.BigqueryTarget), 'Output should be a bigquery target, not %s' % (output)
        bq_client = self.output(tableId).client


        job = {
            'projectId': self.output(tableId).table.project_id,
            'configuration': {
                'query': {
                    'query': query,
                    'priority': self.query_mode,
                    'useLegacySql': True,
                    'destinationTable': {
                        'projectId': "<project>",
                        'datasetId': "<dataset>",
                        'tableId': tableId,
                    },
                    'allowLargeResults': True,
                    'createDisposition': self.create_disposition,
                    'writeDisposition': self.write_disposition,
                }
            }
        }

        bq_client.run_job(self.output(tableId).table.project_id, job, dataset=self.output(tableId).table.dataset)

def loadDaily():
  global key_views, days_ago, is_overwrite, hour_offset

  flow = dailyBuilds()
  is_overwrite = True

  # attempt to build daily tables for each of the days

# TODO: CHANGE THIS AS THE MAXIMUM DAYS AGO THAT THE CODE NEEDS TO LOOK BACK
  while days_ago < 90:
    print('days: ',days_ago)

    did_load_required = False #reset required views flag
    for view in key_views.keys():
      view_name = 'view_'+view
      viewTable = dataset.table(name=view_name)
      if (not viewTable.exists()):
        print('DAILY FAILS - NO VIEW: '+view_name)
        continue

      recorded_date = (datetime.today()- timedelta(days=days_ago)).strftime('%Y%m%d')
      tableId = """daily_{view}_{date}""".format(view=view,date=recorded_date)
      table = dataset.table(name=tableId)
      if table.exists():  #do not overwrite existing tables
        print('DAILY ALREADY EXISTS - SKIPPING '+tableId)
        continue
      if not did_load_required:
        loadRequired()  #parametrized required views
        did_load_required = True #we do not need to load it again for this date
      destinationTable = """[<project>.<dataset>.{tableId}]""".format(tableId=tableId)
      query = """
      # __ETL__ {table_name}
      SELECT
        DATE(DATE_ADD(TIMESTAMP(CONCAT(DATE( DATE_ADD( CURRENT_TIMESTAMP(), -{hour_offset}, 'HOUR')), ' 00:00:00')), -{days_ago}, 'DAY')) AS snapshot_date
        , *
      FROM [<project>.<dataset>.view_{view}]
      """.format(view=view, table_name=destinationTable,days_ago=days_ago,hour_offset=hour_offset)

      flow.saveTable(query,tableId)

    days_ago = days_ago + 1

def cleanTables(dataset_name, project=None):
    global key_views
    """Lists all of the tables in a given dataset.

    If no project is specified, then the currently active project is used.
    """
    bigquery_client = google.cloud.bigquery.Client(project=project)
    dataset = bigquery_client.dataset(dataset_name)

    if not dataset.exists():
        print('Dataset {} does not exist.'.format(dataset_name))
        return

    tables = []
    page_token = None


    def deleteTable(table):
      print('WARNING!!!')
      print('Deleting daily table: '+table.name)
      table.delete()

    if len(tables_to_delete) > 0:
      # if dates are specified, only delete specific dates
      for view in key_views:
        print('DELETING days:'+','.join(tables_to_delete)+' in dailys: '+view)
        for date in tables_to_delete:
          tableId = """daily_{view}_{date}""".format(view=view,date=date)
          table = dataset.table(name=tableId)
          if table.exists():
            deleteTable(table)
          else:
            print('TABLE '+table.name+' does NOT exist - cannot delete')
    else:
      while True:
        results, page_token = dataset.list_tables(page_token=page_token)
        tables.extend(results)

        if not page_token:
            break
      assert tables
      for table in tables:
        for view in key_views:
          if table.name.startswith('daily_'+view+'_'):
              deleteTable(table)

def dateRange(matches):
  global date_dependent_views, current_view
  date_dependent_views[current_view] = 1
  return """{days_ago}""".format(days_ago=days_ago)

def dateRange2(matches):
  global date_dependent_views, current_view
  date_dependent_views[current_view] = 1
  return """{hour_offset}""".format(hour_offset=hour_offset)

def dateParameter(matches):
  global date_dependent_views, current_view
  date_dependent_views[current_view] = 1
  print('replaced Data Parameter in '+current_view)
  recorded_date = (datetime.today()- timedelta(days=days_ago)).strftime('%Y-%m-%d')
  return """{date}""".format(date=recorded_date)

def saveView(view_name,full_path,is_required=False):
  global done_tables
  global skipped_tables_count
  global is_overwrite
  # some views can be referenced as dependencies multiple times, so ignore them on subsequent attempts
  if view_name in done_tables:
    return
  else:
    done_tables[view_name] = 1
  # ignore non view_ files
  if not os.path.isfile(full_path):
    print('WARNING: skipping '+re.search('.*\/([a-z_0-9]+).sql',full_path).group(1))
    return
  print('unpacking file:'+full_path)
  with open(full_path) as f:
    view_query = f.read()
    view_query = re.sub('{days_ago}',dateRange, view_query)
    view_query = re.sub('{hour_offset}',dateRange2, view_query)
    view_query = re.sub('{date}',dateParameter, view_query)
    table = dataset.table(name=view_name)
    table.view_query = view_query
    try:
      if (not table.exists()):
        print('creating a new view: '+view_name)
        table.create()
        assert table.exists()
      elif is_overwrite or is_required:
        print('updating an old view: '+view_name)
        table.patch(
          view_query=view_query
        )
        assert table.view_query == view_query
      else:
        print('view already exists: '+view_name)
    except google.cloud.exceptions.BadRequest as err:
      #attempt to continue - do not hold other views just because one breaks down
      skipped_tables_count = skipped_tables_count + 1
      print("400 - skipping '{view_name}': {err}".format(view_name=view_name,err=err))
      print('skipped count: ',skipped_tables_count)
    except google.cloud.exceptions.NotFound as err:
      skipped_tables_count = skipped_tables_count + 1
      print("404 - skipping '{view_name}': {err}".format(view_name=view_name,err=err))
      print('skipped count: ',skipped_tables_count)

def saveViews(views,is_key_view_tree=False):
  global key_views,current_view, VIEW_FOLDER
  for view in views.keys():
    is_key = (view in key_views) or is_key_view_tree
    saveViews(views[view],is_key)  # save children first

    current_view = 'view_'+view
    filename = os.path.join(VIEW_FOLDER, current_view+'.sql')
    if is_key:
      saveView(current_view,filename)

firstTimeLoadingRequired = True

def loadRequired():
  print('loading required')
  global current_view, VIEW_FOLDER, firstTimeLoadingRequired, done_tables
  done_tables = {}
  #first load new required views
  for filename in os.listdir(VIEW_FOLDER+'required/'):
      full_path = os.path.join(VIEW_FOLDER+'required/',filename)
      current_view = re.search('.*\/([a-z_0-9]+).sql',full_path).group(1)
      if firstTimeLoadingRequired or (current_view in date_dependent_views):
        print('req view: '+current_view)
        saveView(current_view,full_path,True)

  firstTimeLoadingRequired = False

def main():
  global current_view
  global dependencies
  global days_ago
  global VIEW_FOLDER
  global tables_to_delete
  if len(sys.argv) >= 4:
    VIEW_FOLDER = sys.argv[1]
    if VIEW_FOLDER.find('/',len(VIEW_FOLDER)-1) == -1:
      VIEW_FOLDER = VIEW_FOLDER+'/'
    days_ago = int(sys.argv[2])
    is_overwrite = (sys.argv[3] == 'True')
    dependency_path = sys.argv[4]
    if len(sys.argv) > 5 and sys.argv[5] == 'clean':
      if len(sys.argv) > 6:
        tables_to_delete = list(sys.argv[6:])
      cleanTables('<dataset>','<project>')
  else:
    print("./bigquery_view_persist.py path_to_views days_ago is_overwrite(True|False)")
    sys.exit()

  with open(dependency_path) as data_file:
      dependencies = json.load(data_file)

  assert dependencies
  # TODO for all dependencies - not just events
  dependencies = dependencies['events']

  loadRequired()
  saveViews(dependencies)

  # materialize tables
  # load dailys
  loadDaily()



if __name__ == '__main__':
  main()
