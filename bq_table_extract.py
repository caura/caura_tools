#!/usr/bin/env python
# from __future__ import with_statement
import re
import os
import sys

def main():
  global VIEW_FOLDER
  if len(sys.argv) >= 2:
    VIEW_FOLDER = sys.argv[1]
    if VIEW_FOLDER.find('/',len(VIEW_FOLDER)-1) == -1:
      VIEW_FOLDER = VIEW_FOLDER+'/'

  tables = {} #dictionary to keep uniques only

  for path, dirs, files in os.walk(VIEW_FOLDER):
    for filename in files:
      fullpath = os.path.join(path, filename)
      with open(fullpath, 'r') as f:

          data = re.findall(r'((<table1>|<table2>)\.[a-z0-9_A-Z]+)',f.read(),re.MULTILINE)
          for table in data:
            tables[table[0]] = 1
  pretty_names = []
  for name in tables:
    pretty_names.append(name)
  print pretty_names
if __name__ == '__main__':
  main()
