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
    return re.match('^[ ]*\$\{TABLE\}.[a-z_0-9]+[ ]*$', field, re.MULTILINE) or re.match('^[ ]*\$\{TABLE\}."[a-z_0-9\.#]+"[ ]*$', field, re.MULTILINE)

  @staticmethod
  def formatFieldName(sqlField):
    return sqlField.replace('${table}.','').replace('${TABLE}.','').replace('.','_').replace('#','').replace('"','')

  def __init__(self, stream, readStream):
    self.writeStream = stream
    self.readStream = readStream
    self.new_tree = []

  def newView(self,element):
    self.new_tree.append({
      'view': element['view']
      , 'fields': []
    })

  @staticmethod
  def isDimensionGroup(field):
    if Parser.TYPE_DIMENSION_GROUP in field:
      return True
    if Parser.TYPE_DIMENSION in field and ('type' in field and field['type'] == 'time'):
      return True
    return False

  def fire(self,field,type,field_orig=None):
    return
    # if type in [Parser.TYPE_DIMENSION,Parser.TYPE_DIMENSION_GROUP] and 'sql' in field:
    #   # if raw reference, then remove a bunch of stuff
    #   if Parser.isRawReference(field['sql']):
    #     # remove absolute reference
    #     field_orig.pop('type', None)
    #     field_orig.pop('sql', None)

    # if type in [Parser.TYPE_MEASURE] and 'sql' in field:
    #   if re.search('[ ]*(\$\{TABLE\}.[a-z_0-9]+)[ ]*', field['sql']):
    #     field_orig['sql'] = re.sub('\$\{TABLE\}.([a-z_0-9]+)',r'${\1}',field['sql'])

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
    # rewrite dimension: some_name
    #         sql: ${TABLE}.some_name
    self.readStream.seek(0)
    # content = self.readStream.read()

    # pattern0 = re.compile('(sql:[\s\t]*\$\{TABLE\}\.)"([a-z0-9_\.#]+)"',re.I | re.MULTILINE)

    # pattern1 = re.compile("(dimension(_group)?:[\s\t]+\"?(?P<dimension_name>[a-z0-9_]+)\"?[\s\t\n]+[^\$]+)^[\s\t]+sql:[\s\t]*\$\{TABLE\}\.\"?(?P=dimension_name)\"?(?![a-z0-9_])[^\n]*",re.I | re.MULTILINE)
    # pattern2 = re.compile('\$\{TABLE\}."?([a-z_0-9\.]+)"?',re.I | re.MULTILINE)
    # pattern3 = re.compile("(view:[\s\t]+([a-z0-9_]+)(?![a-z0-9_]))",re.I | re.MULTILINE)

    # def nestedFields(matches):
    #   # print (matches.group(2))
    #   return "{sql_part}\"{formated_field}\"".format(sql_part=matches.group(1),formated_field=matches.group(2).replace('.','_').replace('#',''))

    # content = re.sub(pattern0,nestedFields, content)

    # content = re.sub(pattern1,r'\1',content)
    # content = re.sub(pattern2,r'${\1}',content)
    # content = re.sub(pattern3,r"\1\n  extends: [raw_\2]\n",content)
    # self.writeStream.write(content)

#
 # Parser that handles raw dump of dimensions
 #
class baseViewParser(Parser):

  def __init__(self, stream, readStream):
    self.writeStream = stream
    self.readStream = readStream
    self.new_tree = []
    self.fields = []
    self.previousKey = ''

  def fire(self,field,type,field_orig=None):
    if type in ['dimension','dimension_group']:
      name = field[type]
      new_field = {}
      # re.search('[ ]*(\$\{TABLE\}.[a-z_]+)[ ]*', field['sql']):
      if 'sql' in field and Parser.isRawReference(field['sql']):
        new_field[type] = Parser.formatFieldName(field['sql']) # e.g. new_field['dimension'] = name
        new_field['sql'] = field['sql']
        if 'type' in field:
          new_field['type'] = field['type']
        self.new_tree[-1]['fields'].append(new_field)
      elif 'sql' not in field and 'sql_case' not in field:
        new_field[type] = name
        new_field['sql'] = '${TABLE}.'+name #Looker allows empty sql references, so create one
        self.new_tree[-1]['fields'].append(new_field)

  def dump(self,tree):
    for directive in self.new_tree:
      view = directive
      name = view['view']
      view['view'] = 'raw_'+name
      view['extension'] = 'required'
    dump(self.new_tree, self.writeStream, default_flow_style=False)


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
        if isinstance(element, dict) and key == 'fields':
          for listener in listeners:
            listener.newView(element)
        processElement(listeners,element[key])


def processView(stream,listeners):
    file_tree = load(stream, Loader=Loader)
    # temp_file = os.path.join('/<project>','temp')
    # with open(temp_file,'w') as raw_f:
    #   dump(file_tree, raw_f, default_flow_style=False)
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
  INPUT_DIRECTORY = ''
  OUTPUT_DIRECTORY = ''
  if len(sys.argv) >= 3:
    INPUT_DIRECTORY = sys.argv[1]
    OUTPUT_DIRECTORY = sys.argv[2]
  else:
    print("./set_builder.py INPUT_LOOKML_FILE OUTPUT_DIRECTORY")
    sys.exit()

  if INPUT_DIRECTORY.find('/',len(INPUT_DIRECTORY)-1) == -1:
    INPUT_DIRECTORY = INPUT_DIRECTORY+'/'
  if OUTPUT_DIRECTORY.find('/',len(OUTPUT_DIRECTORY)-1) == -1:
    OUTPUT_DIRECTORY = OUTPUT_DIRECTORY+'/'

  for filename in os.listdir(INPUT_DIRECTORY):
    if filename.endswith(".view.lookml"):
      # print filename
      full_path = os.path.join(INPUT_DIRECTORY,filename)
      stream = file(full_path, 'r')

      raw_schema_file = os.path.join(OUTPUT_DIRECTORY,'raw.'+filename)
      modified_file = os.path.join(OUTPUT_DIRECTORY,filename)

      # start writing out the output for these files immediately
      listeners = []
      with open(raw_schema_file,'w') as raw_f:
        with open(modified_file,'w') as mod_f:
          listeners.append(baseViewParser(raw_f,stream))
          listeners.append(Parser(mod_f,stream))
          processView(stream,listeners)


if __name__ == '__main__':
  main()
