#!/usr/bin/env python
import sys
import os
import re
def main():
  INPUT_DIRECTORY = ''
  OUTPUT_DIRECTORY = ''
  if len(sys.argv) >= 3:
    INPUT_DIRECTORY = sys.argv[1]
    OUTPUT_DIRECTORY = sys.argv[2]
  else:
    print("./field_matcher.py INPUT_LOOKML_FILE OUTPUT_DIRECTORY")
    sys.exit()
  fields = {}

  table_specific_sets = {}
  files = []
  if INPUT_DIRECTORY.find('/',len(INPUT_DIRECTORY)-1) == -1:
    INPUT_DIRECTORY = INPUT_DIRECTORY+'/'

    for filename in os.listdir(INPUT_DIRECTORY):
      files.append(filename)
      new_set = []
      # print filename
      full_path = os.path.join(INPUT_DIRECTORY,filename)
      with open(full_path,'r') as stream:
        for line in stream:
          field = line.replace(',','').replace(' ','').replace('\n','')
          if re.match('.*[a-z]+.*',field):
            fields[field] = 1
            new_set.append(field)
      table_specific_sets[filename] = new_set

  for filename in files:
    raw_output_file = os.path.join(OUTPUT_DIRECTORY,'out.'+filename)
    table_set = table_specific_sets[filename]
    line_of_fields = ''
    for field in fields:
      if field in table_set:
        line_of_fields += ','+field #this field is in this table
      else:
        line_of_fields += ',NULL AS '+field
    with open(raw_output_file,'w') as raw_f:
      raw_f.write(line_of_fields)

  # all
  line_of_fields = ''
  raw_output_file = os.path.join(OUTPUT_DIRECTORY,'out.all.txt')
  for field in fields:
    line_of_fields += ',NULL AS '+field #this field is in this table
  with open(raw_output_file,'w') as raw_f:
    raw_f.write(line_of_fields)

if __name__ == '__main__':
  main()
