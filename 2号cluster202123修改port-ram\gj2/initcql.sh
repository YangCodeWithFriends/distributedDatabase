#!/bin/bash
###
 # @Author: YuhaoWU
 # @Date: 2022-10-31 15:14:06
 # @LastEditors: YuhaoWU
 # @LastEditTime: 2022-11-01 01:23:59
 # @Description: 
### 

ssh cs4224j@xcnd22.comp.nus.edu.sg \
"sh -c 'cd ~/yugabyte-2.14.1.0/bin && ./ycqlsh 192.168.48.242 9040 -u cassandra -p cassandra -f ~/db_init_csql_SoCluster/db_create_ycql_cluster_addidx.cql'"

sqlShellPath=/home/stuproj/cs4224j/yugabyte-2.14.1.0/bin/ycqlsh
cqlfilePath=/home/stuproj/cs4224j/db_init_csql_SoCluster/db_create_ycql_cluster_addidx.cql

echo "xcnd$1.comp.nus.edu.sg"
echo $2

${sqlShellPath} "xcnd$1.comp.nus.edu.sg" $2 -u cassandra -p cassandra -f ${cqlfilePath}

# full params names
# ${sqlShellPath} "xcnd$1.comp.nus.edu.sg" $2 --username=cassandra  --dbname=cassandra --file=${cqlfilePath}



./ycqlsh 192.168.48.242 9040 -u cassandra -p cassandra -f ~/db_init_csql_SoCluster/db_create_ycql_cluster_addidx.cql
