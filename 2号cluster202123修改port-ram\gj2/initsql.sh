#!/bin/bash
###
 # @Author: YuhaoWU
 # @Date: 2022-10-31 15:14:00
 # @LastEditors: YuhaoWU
 # @LastEditTime: 2022-10-31 20:53:46
 # @Description: 
### 
sqlShellPath=/home/stuproj/cs4224j/yugabyte-2.14.1.0/bin/ysqlsh
sqlfilePath=~/db_init_csql_SoCluster/db_create_ysql_cluster_addidx.sql

echo "xcnd$1.comp.nus.edu.sg"
echo $2

${sqlShellPath} -h "xcnd$1.comp.nus.edu.sg" -p $2 -U yugabyte -d yugabyte -f ${sqlfilePath}

# full params names
# ${sqlShellPath} --host="xcnd$1.comp.nus.edu.sg" --port=$2 --username=yugabyte  --dbname=yugabyte --file=$sqlfile

