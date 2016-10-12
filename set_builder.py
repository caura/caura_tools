#!/usr/bin/env python
from __future__ import division
import sys
import os
import json
import re
import copy
from yaml import load, dump

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

class Parser():
  TYPE_DIMENSION = 'dimension'
  TYPE_DIMENSION_GROUP = 'dimension_group'
  TYPE_MEASURE = 'measure'
  TYPE_FILTER = 'filter'

  DEFAULT_TIMEFRAMES = ['date', 'day_of_month', 'day_of_week', 'day_of_week_index', 'day_of_year', 'hour', 'hour_of_day', 'minute', 'month', 'month_name', 'month_num', 'quarter', 'quarter_of_year', 'time', 'time_of_day', 'week', 'week_of_year', 'year' ]

  @staticmethod
  def isRawReference(field):
    return re.match('^ *\$\{TABLE\}.[a-z_0-9]+ *$', field, re.MULTILINE)

  def __init__(self, stream):
    self.writeStream = stream

  @staticmethod
  def isDimensionGroup(field):
    if Parser.TYPE_DIMENSION_GROUP in field:
      return True
    if Parser.TYPE_DIMENSION in field and ('type' in field and field['type'] == 'time'):
      return True
    return False

  def fire(self,field,type,field_orig=None):
    if type in [Parser.TYPE_DIMENSION,Parser.TYPE_DIMENSION_GROUP] and 'sql' in field:
      # if raw reference, then remove a bunch of stuff
      if Parser.isRawReference(field['sql']):
        # remove absolute reference
        field_orig.pop('type', None)
        field_orig.pop('sql', None)

    if type in [Parser.TYPE_MEASURE] and 'sql' in field:
      if re.search('[ ]*(\$\{TABLE\}.[a-z_0-9]+)[ ]*', field['sql']):
        field_orig['sql'] = re.sub('\$\{TABLE\}.([a-z_0-9]+)',r'${\1}',field['sql'])

  def extractGroup(self,field):
    set = []
    name = field['dimension_group'] if Parser.TYPE_DIMENSION_GROUP in field else field['dimension']
    if 'timeframes' in field:
      for dim in field['timeframes']:
        set.append(name+'_'+dim)
    else:
      # default timeframes
      for dim in Parser.DEFAULT_TIMEFRAMES:
        set.append(name+'_'+dim)
    return set

  def dump(self,tree):
    dump(tree, self.writeStream, default_flow_style=False)


#
 # Parser that handles sets
 #
class baseViewParser(Parser):

  def __init__(self, stream):
    self.writeStream = stream
    self.fields = []

  def fire(self,field,type,field_orig=None):
    if type in ['dimension','dimension_group'] and 'sql' in field:
      name = field[type]
      # re.search('[ ]*(\$\{TABLE\}.[a-z_]+)[ ]*', field['sql']):
      if field['sql'].startswith('${TABLE}.') and Parser.isRawReference(field['sql']):
        new_field = {}
        new_field[type] = name # e.g. new_field['dimension'] = name
        if 'type' in field:
          new_field['type'] = field['type']
        if 'sql' in field:
          new_field['sql'] = field['sql']
        else:
          new_field['sql'] = '${TABLE}.'+name
        self.fields.append(new_field)

  def dump(self,tree):
    new_tree = []
    for directive in tree:
      new_tree.append({
        'view': {}
        , 'fields': self.fields
      })  # tree is blank at the beginning

    dump(new_tree, self.writeStream, default_flow_style=False)

#
 # Parser that handles sets
 #
class setParser(Parser):
  sets = {
    'Clickstream': 1
    , 'Sponsorships': 1
    , 'User Activity': 1
    , 'Consumer': 1
    , 'Email': 1
  }

  DIMENSION_ALL_TYPE = 1
  DIMENSION_GROUP_ALL_TYPE = 2

  def __init__(self, stream):
    self.writeStream = stream

    self.hiddenFields = {
      'dimension': []
      , 'dimension_group': []
    }

    self.resulting_set = {
        'dimensions': {}
        , 'measures': {}
        , 'all': {
          'dimension': {}
          , 'dimension_group': {}
          , 'filter': {}
          , 'measure': {}
        }
      }

    for key in setParser.sets.keys():
      self.resulting_set['dimensions'][key] = {}
      self.resulting_set['measures'][key] = {}

  def fire(self,field,type,field_orig=None):
    name = field[type]
    if type in [Parser.TYPE_DIMENSION,Parser.TYPE_DIMENSION_GROUP,Parser.TYPE_MEASURE]: #,Parser.TYPE_FILTER]:
      if Parser.isDimensionGroup(field):
        dimensions = self.extractGroup(field)
        for dim in dimensions:
          self.resulting_set['all'][Parser.TYPE_DIMENSION][dim] = 2
      else:
        self.resulting_set['all'][type][name] = 1

      # hide dimensions and measures
      if type in [Parser.TYPE_DIMENSION]:
        self.hiddenFields['dimension'].append(name)
      if type in [Parser.TYPE_DIMENSION_GROUP]:
        self.hiddenFields['dimension_group'].append(name)

      matching_category = self.isFilterMatch(field) or self.isLabelMatch(field)
      if matching_category:
        if Parser.isDimensionGroup(field):
          dimensions = self.extractGroup(field)
          for dim in dimensions:
            self.resulting_set['dimensions'][matching_category][dim] = 2  # since it is part of a group
        elif type == Parser.TYPE_DIMENSION:
          self.resulting_set['dimensions'][matching_category][field[type]] = 1
        elif type == Parser.TYPE_MEASURE:
          self.resulting_set['measures'][matching_category][field[type]] = 1
          # # now add all the filter dependencies
          if 'filters' in field:
            for filter_field in field['filters'].keys():
              self.resulting_set['measures'][matching_category][filter_field] = 1



  #
   # returns set name if match, otherwise None
   #
  def isFilterMatch(self,field):
    #TODO
    return None

  #
   # returns set name if match, otherwise None
   #
  def isLabelMatch(self,field):
    if 'view_label' in field:
      for category in self.sets.keys():
        if category in field['view_label']:
          return category
    return None

  def dump(self,tree):
    self.writeStream.write('\tsets:\n')
    # for category in self.sets.keys():
    #   lookmlcatname = category.lower().replace(" ", "")
    #   self.writeStream.write('\t\tdim_'+lookmlcatname+':\n')
    #   for field in self.resulting_set['dimensions'][category].keys():
    #     self.writeStream.write('\t\t\t- '+field+'\n')
    #   self.writeStream.write('\t\tmes_'+lookmlcatname+':\n')
    #   for field in self.resulting_set['measures'][category].keys():
    #     self.writeStream.write('\t\t\t- '+field+'\n')
    #   self.writeStream.write('\t\t'+lookmlcatname+':\n')
    #   self.writeStream.write('\t\t\t- dim_'+lookmlcatname+'*\n')
    #   self.writeStream.write('\t\t\t- mes_'+lookmlcatname+'*\n')

    # self.writeStream.write('\t\tall_dim:\n')
    # for field in self.resulting_set['all']['dimension'].keys():
    #   self.writeStream.write('\t\t\t- '+field+'\n')
    self.writeStream.write('\t\tall_measures:\n')
    for field in self.resulting_set['all']['measure'].keys():
      self.writeStream.write('\t\t\t- '+field+'\n')
    # for field in self.resulting_set['all']['dimension_group']:
    #   self.writeStream.write('\t\t\t- '+field+'\n')
    for field in self.resulting_set['all']['filter'].keys():
      self.writeStream.write('\t\t\t- '+field+'\n')

    self.writeStream.write('\tfields:\n')

    wrote_groups = {}

    for field in self.hiddenFields['dimension']:
      self.writeStream.write('\t- dimension: '+field+'\n\t\thidden: true\n')
    for field in self.hiddenFields['dimension_group']:
      self.writeStream.write('\t- dimension_group: '+field+'\n\t\thidden: true\n')



def processElement(listeners,element):
  if isinstance(element, list):
    for el in element:
      processElement(listeners,el)
  elif isinstance(element, dict):
    element_copy = copy.deepcopy(element)
    for key in element.keys():
      if key in ['dimension','dimension_group','filter','measure']:
        for listener in listeners:
          listener.fire(element_copy,key,element)
        break;
      else:
        processElement(listeners,element[key])


def processView(stream,listeners):
    file_tree = load(stream, Loader=Loader)
    processElement(listeners,file_tree)
    for listener in listeners:
      listener.dump(file_tree)
    stream.close()

#
 # Outputs 3 files:
 # - raw data model (which has only {TABLE}. references, types)
 # - a new copy of the original LookML file with all {TABLE} references replaced with {} names
 # - a set file based on filter matches
 #/
def main():
  INPUT_FILE = ''
  OUTPUT_DIRECTORY = ''
  if len(sys.argv) >= 3:
    INPUT_FILE = sys.argv[1]
    OUTPUT_DIRECTORY = sys.argv[2]
  else:
    print("./set_builder.py INPUT_LOOKML_FILE OUTPUT_DIRECTORY")
    sys.exit()

  if OUTPUT_DIRECTORY.find('/',len(OUTPUT_DIRECTORY)-1) == -1:
    OUTPUT_DIRECTORY = OUTPUT_DIRECTORY+'/'

  file_path = os.path.realpath(INPUT_FILE)
  stream = file(file_path, 'r')

  raw_schema_file = os.path.join(OUTPUT_DIRECTORY,'raw_schema.lookml')
  modified_file = os.path.join(OUTPUT_DIRECTORY,'mod_file.lookml')
  set_file = os.path.join(OUTPUT_DIRECTORY,'set_file.lookml')

  # start writing out the output for these files immediately
  listeners = []
  with open(raw_schema_file,'w') as raw_f:
    with open(modified_file,'w') as mod_f:
      with open(set_file,'w') as set_f:
        listeners.append(baseViewParser(raw_f))
        listeners.append(Parser(mod_f))
        listeners.append(setParser(set_f))
        processView(stream,listeners)


if __name__ == '__main__':
  main()
