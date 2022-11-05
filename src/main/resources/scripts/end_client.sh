#!/bin/bash

for i in $(seq 0 1 4)
do
  echo "Kill java process at xcnd2$i"
  ssh cs4224j@xcnd2$i.comp.nus.edu.sg \
  "sh -c 'pkill -f yugabyte-simple-java-app-1.0-SNAPSHOT.jar'"
done

