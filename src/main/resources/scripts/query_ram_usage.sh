#!/bin/bash

for i in $(seq 20 1 24)
do
  echo "Query ramdisk usage log from xcnd$i"
#  grep -i "Number of transaction" YCQL-log-$i.txt | tail -n 1
  ssh cs4224j@xcnd$i.comp.nus.edu.sg \
    "sh -c 'cd /mnt/ramdisk && du -h --max-depth=0 *'"
done

#ssh cs4224j@xcnd20.comp.nus.edu.sg \
#    "sh -c 'cd /mnt/ramdisk && du -h --max-depth=0 *'"
#
#ssh cs4224j@xcnd21.comp.nus.edu.sg \
#    "sh -c 'cd /mnt/ramdisk && du -h --max-depth=0 *'"
#
#ssh cs4224j@xcnd22.comp.nus.edu.sg \
#    "sh -c 'cd /mnt/ramdisk && du -h --max-depth=0 *'"
#
#ssh cs4224j@xcnd23.comp.nus.edu.sg \
#    "sh -c 'cd /mnt/ramdisk && du -h --max-depth=0 *'"
#
#ssh cs4224j@xcnd24.comp.nus.edu.sg \
#    "sh -c 'cd /mnt/ramdisk && du -h --max-depth=0 *'"