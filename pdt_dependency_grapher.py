#!/usr/bin/env python
from __future__ import division
import csv
import re
import sys
import glob
import os
import math
import json
from yaml import load

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

INPUT_DIRECTORY = ''
OUTPUT_DIRECTORY_SQL = ''
OUTPUT_DIRECTORY_LOOKML = ''

# maps view_names to their file names
file_mapping = {}

pdt_properties = {}

PDT_NAME = """${{{view_name}.SQL_TABLE_NAME}}"""

MATERIAL_TABLE_NAME = """[<project>:<dataset>.view_{view_name}]"""

DYNAMIC_PDT_REFERENCE = """[<project>:<dataset>.pdtref_{view_name}]"""
#TODO need to create views for this dynamically

# of rows allowed for a PDT
MAINTENANCE_SIZE = 2000000

#TODO view name is cut-off in size.csv - need to revisit how those are exported
sizes = {}
def loadPDTSizes(filename):
  with open(filename) as file:
    reader = csv.reader(file)
    #skip header
    reader.next()
    for row in reader:
      sizes[row[0]] = row[2]
    assert len(row) == 4 #four columns in the size-supplied file

def parseDT(view,dependency_tree):
  query = view['derived_table']['sql']
  is_pdt = ('sql_trigger_value' in view['derived_table']) or ('persist_for' in view['derived_table'])
  if view['view'] not in pdt_properties:
    pdt_properties[view['view']] = {}
  pdt_properties[view['view']]["is_pdt"] = is_pdt
  pdt_properties[view['view']]["size"] = sizes.pop(view['view'],None)
  pdt_properties[view['view']]["has_time_dependent_tables"] = False

  dependencies = {}
  has_time_dependent_tables = False

  matches = re.findall(r"\$\{([a-z_0-9]+).SQL_TABLE_NAME\}",query,re.MULTILINE)
  for match in matches:
    dependencies[match] = {}

  matches = re.search("TABLE_DATE_RANGE",query,re.MULTILINE)
  if matches:
    pdt_properties[view['view']]['has_time_dependent_tables'] = True

  dependency_tree[view['view']] = dependencies
  # make sure children of a DT are always represented as roots
  # if not is_pdt:

def depBuilder(dependency_tree,top_parent=0):
  if top_parent == 0:
    top_parent = dependency_tree
  has_time_dependent_tables = False
  for view_name in dependency_tree.keys():
    print(view_name)
    # if it is a child, always copy from dependencies from the topmost tree
    try:
      assert view_name in top_parent
    except AssertionError:
      print('the view with a referenced PDT does not exist - check LookML changes')
      exit(1)
    dependency_tree[view_name] = top_parent[view_name]
    print('build children')
    is_time_dependent = depBuilder(dependency_tree[view_name],top_parent)
    pdt_properties[view_name]['has_time_dependent_tables'] = pdt_properties[view_name]['has_time_dependent_tables'] or is_time_dependent
    if pdt_properties[view_name]['has_time_dependent_tables']:
      has_time_dependent_tables = True # if at least one dependent table is time-dependent, then flag the parent table as such
  return has_time_dependent_tables

def processModels(stream,end_points):
    model = load(stream, Loader=Loader)
    for directive in model:
      if 'explore' in directive:
        if 'from' in directive:
          end_points[directive['from']] = 1
        elif ('extension' not in directive) or directive['extension'] != 'required':
          end_points[directive['explore']] = 1
      if 'joins' in directive:
        for join in directive['joins']:
          if 'join' in join:
            if 'from' in join:
              end_points[join['from']] = 1
            else:
              end_points[join['join']] = 1
    return end_points

def processViews(stream, dependency_tree,file_reference):
  views = load(stream, Loader=Loader)
  # can have multiple views defined in each file
  for view in views:
    if ('derived_table' in view) and ('view' in view):
      # save a reference to a file
      file_mapping[view['view']] = file_reference
      parseDT(view,dependency_tree)

  return dependency_tree

def toD3Json(dependency_tree,parent='<project>'):
  new_store = []
  for key in dependency_tree.keys():
    element = {
        "name": key
        , "parent": parent
      }
    if dependency_tree[key]:
      element["children"] = toD3Json(dependency_tree[key],key)
    new_store.append(
      element
    )
  return new_store
def performReplacement(text,rep):
  assert isinstance(rep, dict)
  rep = dict((re.escape(k), v) for k, v in rep.iteritems())
  pattern = re.compile("|".join(rep.keys()))
  return pattern.sub(lambda m: rep[re.escape(m.group(0))], text)

def getSQLfromView(view_name):
  assert view_name in file_mapping
  filename = file_mapping[view_name]
  stream = file(filename, 'r')
  # find the relevant view in this file
  views = load(stream, Loader=Loader)
  for view in views:
    if view['view'] == view_name:
      break;
  query = view['derived_table']['sql']
  stream.close()
  return query

def printView(view_name,substitutions,is_replacable=True):
  query = getSQLfromView(view_name)
  if is_replacable:
    new_file = OUTPUT_DIRECTORY_SQL+'view_'+view_name+'.sql'
  else:
    new_file = OUTPUT_DIRECTORY_LOOKML+view_name+'.sql'
  if os.path.isfile(new_file):
    return
  # substitute strings:
  if substitutions:
    query = performReplacement(query,substitutions)
  # with open(new_file,'w') as f:
  #   f.write(query)

def isPDTReplacable(view_name):
  if (pdt_properties[view_name]['is_pdt']
    and
    pdt_properties[view_name]['size'] > MAINTENANCE_SIZE
    and
    pdt_properties[view_name]['has_time_dependent_tables']):
    return True
  else:
    return False

def whichReplacementString(view_name,parent_view):

  if isPDTReplacable(view_name):
    # print('pdt '+view_name+' is too large and we can do something about it')
    return MATERIAL_TABLE_NAME
  elif isPDTReplacable(parent_view):
    # if pdt_properties[view_name]['is_pdt'] and pdt_properties[view_name]['size'] > MAINTENANCE_SIZE:
    #   # print('pdt '+view_name+' is too large and there IS NOTHING we do about it')
    # elif pdt_properties[view_name]['is_pdt']:
    #   # print('pdt '+view_name+' is not large')
    return DYNAMIC_PDT_REFERENCE
  else:
      return PDT_NAME

def dtToView(view_name,dependency_tree,out):
  dependencies = dependency_tree[view_name]
  if not dependencies:
    out(view_name,{},isPDTReplacable(view_name))
  else:
    substitutions = {}
    for view in dependencies.keys():
      dtToView(view,dependencies,out)
      substitutions[PDT_NAME.format(view_name=view)] = whichReplacementString(view,view_name).format(view_name=view)
      # dependencies.pop(view, None)
    out(view_name,substitutions,isPDTReplacable(view_name))


def main():
  #set inputs
  verbose = False
  if len(sys.argv) >= 3:
    INPUT_DIRECTORY = sys.argv[1]
    if INPUT_DIRECTORY.find('/',len(INPUT_DIRECTORY)-1) == -1:
      INPUT_DIRECTORY = INPUT_DIRECTORY+'/'
    OUTPUT_DIRECTORY = sys.argv[2]
    if OUTPUT_DIRECTORY.find('/',len(OUTPUT_DIRECTORY)-1) == -1:
      OUTPUT_DIRECTORY = OUTPUT_DIRECTORY+'/'

    OUTPUT_DIRECTORY_SQL = OUTPUT_DIRECTORY+'sql/'
    OUTPUT_DIRECTORY_LOOKML = OUTPUT_DIRECTORY+'lookml/'

    csv_size_file = sys.argv[3]
  else:
    print("./pdt_dependency_grapher.py INPUT_DIRECTORY OUTPUT_DIRECTORY FULL_PATH_TO_PDT_SIZE_FILE")
    sys.exit()

  loadPDTSizes(csv_size_file)
  assert sizes

  dependency_tree = {}
  end_points = {}

  for filename in os.listdir(INPUT_DIRECTORY):
    if filename.endswith(".model.lookml") or filename.endswith(".view.lookml") or filename.endswith(".base.lookml"):
      full_path = os.path.join(INPUT_DIRECTORY,filename)
      stream = file(full_path, 'r')
      if (filename.endswith(".model.lookml") or filename.endswith(".base.lookml")) and filename.startswith('rsdw.'):
        # TODO: enable all models?
        end_points = processModels(stream,end_points)
      else:
        dependency_tree = processViews(stream,dependency_tree,full_path)
      stream.close()
  # clean up Looker unkept PDTs?
  # assert not sizes #make sure we have covered all PDTs pulled from a db

  #2nd step: build out a full free
  depBuilder(dependency_tree)

  #3rd step: remove unnecessery roots
  assert end_points #fail only if no joins or explores in the model
  before = len(dependency_tree.keys())
  for view_name in dependency_tree.keys():
    if view_name not in end_points:
      dependency_tree.pop(view_name, None)

  after = len(dependency_tree.keys())
  assert before > after


  assert dependency_tree #are there still roots left to process
  #4rd step: record dependencies to views
  for view_name in dependency_tree.keys():
      dtToView(view_name,dependency_tree,printView)

  #5th step dump dependency record

  temp_file = OUTPUT_DIRECTORY+'dependency.json'
  # temp_file = OUTPUT_DIRECTORY+'dependency-friendly.json'
  with open(temp_file,'w') as f:
  #   tree = [{
  #     "name": "<project>"
  #     , "parent": "null"
  #     , "children": toD3Json(dependency_tree)
  #   }]
    # f.write(json.dumps(tree))
    f.write(json.dumps(dependency_tree))


if __name__ == '__main__':
  main()
