#!/bin/bash

for i in $(seq 20 1 24)
do
  echo "Query server load from xcnd$i"
  ssh cs4224j@xcnd$i.comp.nus.edu.sg \
      "sh -c 'uptime'"
done

# ./query_sql.sh | awk '{sum += $NF}; END {print sum}'