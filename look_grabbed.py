#!/usr/bin/env python
import sys
import os
import re
def main():
  INPUT_FILE = ''
  OUTPUT_FILE = ''
  if len(sys.argv) >= 2:
    INPUT_FILE = sys.argv[1]
  else:
    print("./link_grabber.py INPUT_HTML_FILE")
    sys.exit()

  looks = []
  with open(INPUT_FILE,'r') as stream:
    matches = re.finditer(r' href="/looks/([0-9]+)"', stream.read())
    for match in matches:
      looks.append(match.group(1))

  print(';'.join([str(x) for x in looks]))

if __name__ == '__main__':
  main()
