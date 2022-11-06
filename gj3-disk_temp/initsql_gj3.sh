#!/bin/bash
###
 # @Author: YuhaoWU
 # @Date: 2022-10-31 15:14:00
 # @LastEditors: YuhaoWU
 # @LastEditTime: 2022-11-01 13:07:18
 # @Description: 
### 

home=/home/stuproj/cs4224j
shellPath=$home/yugabyte-2.14.1.0/bin/ysqlsh
filePath=$home/db_init_csql_SoCluster/db_create_ysql_cluster_addidx.sql

username=cs4224j@
# serverhost=xcnd24.comp.nus.edu.sg
loadIP=192.168.48.$1
# serverip="192.168.48.242"
cqlport=5432

echo "loading into ${username}${loadIP}"


ssh ${username}${loadIP} \
"sh -c '${shellPath} --host=${loadIP} --port=${cqlport} --username=yugabyte  --dbname=yugabyte --file=${filePath}'"

echo "loaded ${loadIP} ${cqlport}"

# ${shellPath} -h "xcnd$1.comp.nus.edu.sg" -p $2 -U yugabyte -d yugabyte -f ${filePath}

# full params names
# ${shellPath} --host="xcnd$1.comp.nus.edu.sg" --port=$2 --username=yugabyte  --dbname=yugabyte --file=$sqlfile

