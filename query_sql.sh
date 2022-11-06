#!/bin/bash

for i in $(seq 0 1 19)
        do
                  #echo "Query YSQL log from thread $i"
                    echo "query from $i $(grep -i "numberOfTxnExecuted" YSQL-log-$i.txt | tail -n 1)"
                done

