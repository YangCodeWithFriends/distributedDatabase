
###
 # @Author: YuhaoWU
 # @Date: 2022-10-30 20:34:26
 # @LastEditors: YuhaoWU
 # @LastEditTime: 2022-11-01 02:17:55
 # @Description: 
### 

yb_bin=/home/stuproj/cs4224j/yugabyte-2.14.1.0/bin
gj3path=/temp/gj3

# echo $yb_bin
# echo $gj3path
# run master
ssh cs4224j@xcnd22.comp.nus.edu.sg \
"sh -c 'mkdir -p ${gj3path}/disk1 ${gj3path}/disk2'"
ssh cs4224j@xcnd22.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-master --flagfile 3ms22.conf > ${gj3path}/disk1/yb-master.out 2>&1 &'"

ssh cs4224j@xcnd23.comp.nus.edu.sg \
"sh -c 'mkdir -p ${gj3path}/disk1 ${gj3path}/disk2'"
ssh cs4224j@xcnd23.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-master --flagfile 3ms23.conf > ${gj3path}/disk1/yb-master.out 2>&1 &'"

ssh cs4224j@xcnd24.comp.nus.edu.sg \
"sh -c 'mkdir -p ${gj3path}/disk1 ${gj3path}/disk2'"

ssh cs4224j@xcnd24.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-master --flagfile 3ms24.conf > ${gj3path}/disk1/yb-master.out 2>&1 &'"

# run t-server
ssh cs4224j@xcnd20.comp.nus.edu.sg \
"sh -c 'mkdir -p ${gj3path}/disk1 ${gj3path}/disk2'"

ssh cs4224j@xcnd20.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-tserver --flagfile 3ts20.conf > ${gj3path}/disk1/yb-tserver.out 2>&1 &'"

ssh cs4224j@xcnd21.comp.nus.edu.sg \
"sh -c 'mkdir -p ${gj3path}/disk1 ${gj3path}/disk2'"

ssh cs4224j@xcnd21.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-tserver --flagfile 3ts21.conf > ${gj3path}/disk1/yb-tserver.out 2>&1 &'"


ssh cs4224j@xcnd22.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-tserver --flagfile 3ts22.conf > ${gj3path}/disk1/yb-tserver.out 2>&1 &'"

ssh cs4224j@xcnd23.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-tserver --flagfile 3ts23.conf > ${gj3path}/disk1/yb-tserver.out 2>&1 &'"

ssh cs4224j@xcnd24.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-tserver --flagfile 3ts24.conf > ${gj3path}/disk1/yb-tserver.out 2>&1 &'"
