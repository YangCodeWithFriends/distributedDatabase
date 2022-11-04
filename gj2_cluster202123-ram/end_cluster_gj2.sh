
###
 # @Author: YuhaoWU
 # @Date: 2022-11-01 16:35:16
 # @LastEditors: YuhaoWU
 # @LastEditTime: 2022-11-01 18:34:06
 # @Description: 
### 

clusterno=2
# master & tserver
ms1=20
ms2=21
ms3=23
# tserver
ts1=22
ts2=24

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