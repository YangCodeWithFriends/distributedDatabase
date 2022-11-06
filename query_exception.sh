#!/bin/bash

for i in $(seq 0 1 19)
	do
	echo "log=$i $(grep -i "exception" $1-log-$i.txt | wc -l)"
	done

