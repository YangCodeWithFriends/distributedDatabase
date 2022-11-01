
###
 # @Author: YuhaoWU
 # @Date: 2022-11-01 16:35:16
 # @LastEditors: YuhaoWU
 # @LastEditTime: 2022-11-01 16:50:41
 # @Description: 
### 

# yb_bin=/home/stuproj/cs4224j/yugabyte-2.14.1.0/bin
# cluster_file_path=/temp/gj3
# username=cs4224j@
# loadIP=192.168.48.$1

clusterno=3
# kill master
ms1=22
ms2=23
ms3=24

ssh cs4224j@xcnd${ms1}.comp.nus.edu.sg \
"sh -c 'pkill -f ${clusterno}ms${ms1}.conf && pkill -f ${clusterno}ts${ms1}.conf'"


ssh cs4224j@xcnd${ms2}.comp.nus.edu.sg \
"sh -c 'pkill -f ${clusterno}ms${ms2}.conf && pkill -f ${clusterno}ts${ms2}.conf'"

ssh cs4224j@xcnd${ms3}.comp.nus.edu.sg \
"sh -c 'pkill -f ${clusterno}ms${ms3}.conf && pkill -f ${clusterno}ts${ms3}.conf'"


# kill t-server
ts1=20
ts2=21
ssh cs4224j@xcnd${ts1}.comp.nus.edu.sg \
"sh -c 'pkill -f ${clusterno}ts${ts1}.conf'"

ssh cs4224j@xcnd${ts2}.comp.nus.edu.sg \
"sh -c 'pkill -f ${clusterno}ts${ts2}.conf'"