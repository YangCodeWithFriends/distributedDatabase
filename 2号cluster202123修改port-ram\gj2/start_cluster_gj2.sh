
###
 # @Author: YuhaoWU
 # @Date: 2022-10-30 20:34:26
 # @LastEditors: YuhaoWU
 # @LastEditTime: 2022-11-01 02:22:20
 # @Description: 
### 

yb_bin=/home/stuproj/cs4224j/yugabyte-2.14.1.0/bin
gj3path=/mnt/ramdisk/gj2

# echo $yb_bin
# echo $gj3path
# run master
ssh cs4224j@xcnd20.comp.nus.edu.sg \
"sh -c 'mkdir -p ${gj3path}/disk1 ${gj3path}/disk2'"
ssh cs4224j@xcnd20.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-master --flagfile 2ms20.conf > ${gj3path}/disk1/yb-master.out 2>&1 &'"

ssh cs4224j@xcnd21.comp.nus.edu.sg \
"sh -c 'mkdir -p ${gj3path}/disk1 ${gj3path}/disk2'"
ssh cs4224j@xcnd21.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-master --flagfile 2ms21.conf > ${gj3path}/disk1/yb-master.out 2>&1 &'"

ssh cs4224j@xcnd23.comp.nus.edu.sg \
"sh -c 'mkdir -p ${gj3path}/disk1 ${gj3path}/disk2'"

ssh cs4224j@xcnd23.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-master --flagfile 2ms23.conf > ${gj3path}/disk1/yb-master.out 2>&1 &'"

# run t-server
ssh cs4224j@xcnd20.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-tserver --flagfile 2ts20.conf > ${gj3path}/disk1/yb-tserver.out 2>&1 &'"

ssh cs4224j@xcnd21.comp.nus.edu.sg \
"sh -c 'mkdir -p ${gj3path}/disk1 ${gj3path}/disk2'"

ssh cs4224j@xcnd21.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-tserver --flagfile 2ts21.conf > ${gj3path}/disk1/yb-tserver.out 2>&1 &'"


ssh cs4224j@xcnd22.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-tserver --flagfile 2ts22.conf > ${gj3path}/disk1/yb-tserver.out 2>&1 &'"

ssh cs4224j@xcnd23.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-tserver --flagfile 2ts23.conf > ${gj3path}/disk1/yb-tserver.out 2>&1 &'"

ssh cs4224j@xcnd24.comp.nus.edu.sg \
"sh -c 'mkdir -p ${gj3path}/disk1 ${gj3path}/disk2'"

ssh cs4224j@xcnd24.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-tserver --flagfile 2ts24.conf > ${gj3path}/disk1/yb-tserver.out 2>&1 &'"
