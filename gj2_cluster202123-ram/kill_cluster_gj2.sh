
###
 # @Author: YuhaoWU
 # @Date: 2022-11-01 16:35:16
 # @LastEditors: YuhaoWU
 # @LastEditTime: 2022-11-01 16:50:13
 # @Description: 
### 

clusterno=2
# kill master
ms1=20
ms2=21
ms3=23

ssh cs4224j@xcnd${ms1}.comp.nus.edu.sg \
"sh -c 'pkill -f ${clusterno}ms${ms1}.conf && pkill -f ${clusterno}ts${ms1}.conf'"


ssh cs4224j@xcnd${ms2}.comp.nus.edu.sg \
"sh -c 'pkill -f ${clusterno}ms${ms2}.conf && pkill -f ${clusterno}ts${ms2}.conf'"

ssh cs4224j@xcnd${ms3}.comp.nus.edu.sg \
"sh -c 'pkill -f ${clusterno}ms${ms3}.conf && pkill -f ${clusterno}ts${ms3}.conf'"

# kill t-server
ts1=22
ts2=24
ssh cs4224j@xcnd${ts1}.comp.nus.edu.sg \
"sh -c 'pkill -f ${clusterno}ts${ts1}.conf'"

ssh cs4224j@xcnd${ts2}.comp.nus.edu.sg \
"sh -c 'pkill -f ${clusterno}ts${ts2}.conf'"