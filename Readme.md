# Open sourcing tools for Looker

The link:
[https://github.com/caura/tools](https://github.com/caura/tools)

## About these Tools

Python/Shell scripts. Use them at your own risk. They are clearly not well-documented or productionized, but some are better than others. More, where that came from. If there is interest in us sharing such scripts, let us know.

## What's included


### ETL

**Problem:** need to upload a simple csv table into a Postgres-like Database (RDS or Redshift)

`upload_tables.sh`
- Minimalistic shell script with some examples

### Administration

**Problem:** Your instance has thousands of Looks. Even determining which ones to delete will take forever. You want to do it programmatically. You have pulled a list of Look ids using iLooker. Great! Now, you have 200 looks to delete - how do you do it?

`looker_content_cleaner.py`
- takes a comma separated file with ids and deletes all the associated looks with the use of an API


### Material-Model Contract

**Problem:** Database schema changes frequently. Many fields don't work when you query them, but Looker does not know about this until query time. But when you re-generate views, you lose derived fields that you had saved in the same lookml files.

`set_builder.py`/`material_layer_split.py` (Legacy LookML)
- parses files to build raw view files with basic non-derived dimension references. <br>
**Input: **<br>
orders.view.lookml<br>
**Output: **<br>
raw_orders.view.lookml<br>
orders.view.lookml<br>

`field_matcher.py`
- scans legacy Lookml files

`table_extract.py`
- extracts names of sql tables


### PDTs (BigQuery only)

**Problem:** Too many PDTS

`pdt_dependency_grapher.py`
- First, need to understand the size of the problem. This builds a json with a dependency tree.

`pdt_graph.html`
- Second, lets visualize it.

`bigquery_view_persist.py`
- Move the SQL code and dependencies downstream: create database views, which can be used to build temporary tables.

`bq_remove_tables.sh`
- removes a lst of tables

### Questions

As long as it won't require us rewriting any code, feel free to reach out with questions. [https://segah.me](https://segah.me)