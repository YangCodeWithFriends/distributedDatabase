
###
 # @Author: YuhaoWU
 # @Date: 2022-10-30 20:34:26
 # @LastEditors: YuhaoWU
 # @LastEditTime: 2022-11-05 21:44:24
 # @Description: 停掉现有的cluster，重新启动cluster, 不包括删除之前的数据
### 

yb_bin=/home/stuproj/cs4224j/yugabyte-2.14.1.0/bin
cluster_file_path=/mnt/ramdisk/gj2

# echo $yb_bin
# echo $cluster_file_path

username=cs4224j@
loadIP=192.168.48.$1

# copy conf files
echo "upload all gj2 conf files"
# bash get current bash file directory
bashCurPath=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "Your local script path is: $bashCurPath"
scp ${bashCurPath}/*.conf ${username}${loadIP}:${yb_bin}


# 停掉如果还在run的集群
${bashCurPath}/end_cluster_gj2.sh


# run master
echo 'start master, 20'

ssh cs4224j@xcnd20.comp.nus.edu.sg \
"sh -c 'mkdir -p ${cluster_file_path}/disk1 ${cluster_file_path}/disk2'"

ssh cs4224j@xcnd20.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-master --flagfile 2ms20.conf > ${cluster_file_path}/disk1/yb-master.out 2>&1 &'"

echo 'start master, 21'
ssh cs4224j@xcnd21.comp.nus.edu.sg \
"sh -c 'mkdir -p ${cluster_file_path}/disk1 ${cluster_file_path}/disk2'"
ssh cs4224j@xcnd21.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-master --flagfile 2ms21.conf > ${cluster_file_path}/disk1/yb-master.out 2>&1 &'"


echo 'start master, 23'
ssh cs4224j@xcnd23.comp.nus.edu.sg \
"sh -c 'mkdir -p ${cluster_file_path}/disk1 ${cluster_file_path}/disk2'"
ssh cs4224j@xcnd23.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-master --flagfile 2ms23.conf > ${cluster_file_path}/disk1/yb-master.out 2>&1 &'"

# run t-server
ssh cs4224j@xcnd20.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-tserver --flagfile 2ts20.conf > ${cluster_file_path}/disk1/yb-tserver.out 2>&1 &'"

ssh cs4224j@xcnd21.comp.nus.edu.sg \
"sh -c 'mkdir -p ${cluster_file_path}/disk1 ${cluster_file_path}/disk2'"

ssh cs4224j@xcnd21.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-tserver --flagfile 2ts21.conf > ${cluster_file_path}/disk1/yb-tserver.out 2>&1 &'"


ssh cs4224j@xcnd22.comp.nus.edu.sg \
"sh -c 'mkdir -p ${cluster_file_path}/disk1 ${cluster_file_path}/disk2'"
ssh cs4224j@xcnd22.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-tserver --flagfile 2ts22.conf > ${cluster_file_path}/disk1/yb-tserver.out 2>&1 &'"

ssh cs4224j@xcnd23.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-tserver --flagfile 2ts23.conf > ${cluster_file_path}/disk1/yb-tserver.out 2>&1 &'"

ssh cs4224j@xcnd24.comp.nus.edu.sg \
"sh -c 'mkdir -p ${cluster_file_path}/disk1 ${cluster_file_path}/disk2'"

ssh cs4224j@xcnd24.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-tserver --flagfile 2ts24.conf > ${cluster_file_path}/disk1/yb-tserver.out 2>&1 &'"
