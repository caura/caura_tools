#!/bin/bash

for i in $(bq ls -n 9999 <schema> | grep <table_name> | awk '{print $1}'); do
  if ["$i" \> "<table_name_date>" ];
    then
    echo $i;
  fi
done;

