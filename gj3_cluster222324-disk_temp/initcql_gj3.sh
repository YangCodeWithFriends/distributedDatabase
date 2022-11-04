#!/bin/bash
###
 # @Author: YuhaoWU
 # @Date: 2022-10-31 15:14:06
 # @LastEditors: YuhaoWU
 # @LastEditTime: 2022-11-01 13:08:30
 # @Description: 
### 


home=/home/stuproj/cs4224j
shellPath=$home/yugabyte-2.14.1.0/bin/ycqlsh
filePath=$home/db_init_csql_SoCluster/db_create_ycql_cluster_addidx.cql

username=cs4224j@
# serverhost=xcnd24.comp.nus.edu.sg
loadIP=192.168.48.$1
# serverip="192.168.48.242"
cqlport=9042


echo "loading into ${username}${loadIP}"

ssh ${username}${loadIP} \
"sh -c '${shellPath} ${loadIP} ${cqlport} -u cassandra -p cassandra -f ${filePath}'"

echo "loaded ${loadIP} ${cqlport}"

# echo $2

# ${shellPath} "xcnd$1.comp.nus.edu.sg" $2 -u cassandra -p cassandra -f ${cqlfilePath}

# full params names
# ${shellPath} "xcnd$1.comp.nus.edu.sg" $2 --username=cassandra  --dbname=cassandra --file=${cqlfilePath}
