
###
 # @Author: YuhaoWU
 # @Date: 2022-11-03 00:27:08
 # @LastEditors: YuhaoWU
 # @LastEditTime: 2022-11-03 00:58:34
 # @Description: 
### 


# master & tserver
ms1=22
ms2=23
ms3=24
# pure tserver
ts1=20
ts2=21


printf " to drop db, input a c (for cql) or s(fro sql): "
read -n 1 char

case $char in
    [c])
        printf "\ndrop dbycql in CQL\n"
        # drop ycql datbase
        ssh cs4224j@xcnd${ms2}.comp.nus.edu.sg \
        "sh -c '~/yugabyte-2.14.1.0/bin/ycqlsh 192.168.48.243 9040 -u cassandra -p cassandra --execute==\'drop database dbycql;\''"
        ;;
    [s])
        printf "\ndrop dbycql in SQL\n"
        # drop ysql datbase
        ssh cs4224j@xcnd${ms2}.comp.nus.edu.sg \
        "sh -c '~/yugabyte-2.14.1.0/bin/ysqlsh -h 192.168.48.241 -p 5450 -U yugabyte -d yugabyte --command=='drop database dbysql;''"
        ;;
    *)
        printf "\nerror, $char is not either cql or sql\n"
esac




