
###
 # @Author: YuhaoWU
 # @Date: 2022-11-01 16:35:16
 # @LastEditors: YuhaoWU
 # @LastEditTime: 2022-11-02 11:26:03
 # @Description: 
### 

# yb_bin=/home/stuproj/cs4224j/yugabyte-2.14.1.0/bin
# cluster_file_path=/temp/gj3
# username=cs4224j@
# loadIP=192.168.48.$1

clusterno=3
# master & tserver
ms1=22
ms2=23
ms3=24
# pure tserver
ts1=20
ts2=21

# 用分号，而不是&& ，因为前一个可能来不及结束，表示失败
# 分号并列，表示： no matter what the exit status of the previous command
# 还是分开写，因为kill进程有延迟! 导致tserver来不及kill
ssh cs4224j@xcnd${ms1}.comp.nus.edu.sg \
"sh -c 'pkill -f ${clusterno}ms${ms1}.conf'"

ssh cs4224j@xcnd${ms1}.comp.nus.edu.sg \
"sh -c 'pkill -f ${clusterno}ts${ms1}.conf'"

ssh cs4224j@xcnd${ms2}.comp.nus.edu.sg \
"sh -c 'pkill -f ${clusterno}ms${ms2}.conf'"

ssh cs4224j@xcnd${ms2}.comp.nus.edu.sg \
"sh -c 'pkill -f ${clusterno}ts${ms2}.conf'"

ssh cs4224j@xcnd${ms3}.comp.nus.edu.sg \
"sh -c 'pkill -f ${clusterno}ms${ms3}.conf'"

ssh cs4224j@xcnd${ms3}.comp.nus.edu.sg \
"sh -c 'pkill -f ${clusterno}ts${ms3}.conf'"


# kill t-server

ssh cs4224j@xcnd${ts1}.comp.nus.edu.sg \
"sh -c 'pkill -f ${clusterno}ts${ts1}.conf'"

ssh cs4224j@xcnd${ts2}.comp.nus.edu.sg \
"sh -c 'pkill -f ${clusterno}ts${ts2}.conf'"