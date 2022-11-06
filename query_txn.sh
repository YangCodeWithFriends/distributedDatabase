#!/bin/bash

for i in $(seq 0 1 19)
        do
                echo "id=$i $(grep -i "type='$2'" $1-log-$i.txt | tail -n 1)"
        done

