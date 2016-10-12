#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
from __future__ import division
from oauth2client.client import GoogleCredentials

import luigi
from luigi.contrib import bigquery

class dailyBuilds(bigquery.BigqueryLoadTask):
    credentials = GoogleCredentials.get_application_default()

    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
    query_mode = bigquery.QueryMode.INTERACTIVE

    def output(self,tableId):
        'output'
        return bigquery.BigqueryTarget("project", "dataset", tableId, bigquery.BigqueryClient(oauth_credentials=self.credentials))

    def saveTable(self,query,tableId):
        print('run')
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
                        'projectId': "project",
                        'datasetId': "dataset",
                        'tableId': tableId,
                    },
                    'allowLargeResults': True,
                    'createDisposition': self.create_disposition,
                    'writeDisposition': self.write_disposition,
                }
            }
        }

        bq_client.run_job(self.output(tableId).table.project_id, job, dataset=self.output(tableId).table.dataset)

if __name__ == '__main__':
    # query = "SELECT 1 AS first_column, 2 AS second_column, 3 AS third_column"
    # x = dailyBuilds()
    # tableId = 'segah2'
    # x.run(query,tableId)
    query = "SELECT 1 AS first_column, 2 AS second_column, 3 AS third_column"
    flow = dailyBuilds()
    tableId = 'segah2'
    flow.saveTable(query,tableId)
