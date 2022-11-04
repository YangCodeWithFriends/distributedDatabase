
###
 # @Author: YuhaoWU
 # @Date: ${ms1}22-10-30 ${ms1}:34:26
 # @LastEditors: YuhaoWU
 # @LastEditTime: 2022-11-03 12:16:16
 # @Description: 停掉现有的cluster，重新启动cluster, 不包括删除之前的数据
### 

yb_bin=/home/stuproj/cs4224j/yugabyte-2.14.1.0/bin
cluster_file_path=/mnt/ramdisk/gj1

# echo $yb_bin
# echo $cluster_file_path

username=cs4224j@
loadIP=192.168.48.$1

# copy conf files
echo "upload all gj1 conf files"
# bash get current bash file directory
bashCurPath=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "Your local script path is: $bashCurPath"
scp ${bashCurPath}/*.conf ${username}${loadIP}:${yb_bin}


# 停掉如果还在run的集群
# ${bashCurPath}/end_cluster_gj1.sh

clusterno='' # 1
# master & tserver
ms1=21
ms2=23
ms3=24
# tserver
ts1=20
ts2=22


# run master
echo 'start master, ${ms1}'
ssh cs4224j@xcnd${ms1}.comp.nus.edu.sg \
"sh -c 'mkdir -p ${cluster_file_path}/disk1 ${cluster_file_path}/disk2'"
ssh cs4224j@xcnd${ms1}.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-master --flagfile ${clusterno}ms${ms1}.conf > ${cluster_file_path}/disk1/yb-master.out 2>&1 &'"
ssh cs4224j@xcnd${ms1}.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-tserver --flagfile ${clusterno}ts${ms1}.conf > ${cluster_file_path}/disk1/yb-tserver.out 2>&1 &'"

echo 'start master, ${ms2}'
ssh cs4224j@xcnd${ms2}.comp.nus.edu.sg \
"sh -c 'mkdir -p ${cluster_file_path}/disk1 ${cluster_file_path}/disk2'"
ssh cs4224j@xcnd${ms2}.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-master --flagfile ${clusterno}ms${ms2}.conf > ${cluster_file_path}/disk1/yb-master.out 2>&1 &'"
ssh cs4224j@xcnd${ms2}.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-tserver --flagfile ${clusterno}ts${ms2}.conf > ${cluster_file_path}/disk1/yb-tserver.out 2>&1 &'"

echo 'start master, ${ms3}'
ssh cs4224j@xcnd${ms3}.comp.nus.edu.sg \
"sh -c 'mkdir -p ${cluster_file_path}/disk1 ${cluster_file_path}/disk2'"
ssh cs4224j@xcnd${ms3}.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-master --flagfile ${clusterno}ms${ms3}.conf > ${cluster_file_path}/disk1/yb-master.out 2>&1 &'"
ssh cs4224j@xcnd${ms3}.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-tserver --flagfile ${clusterno}ts${ms3}.conf > ${cluster_file_path}/disk1/yb-tserver.out 2>&1 &'"


# run rest t-server
ssh cs4224j@xcnd${ts1}.comp.nus.edu.sg \
"sh -c 'mkdir -p ${cluster_file_path}/disk1 ${cluster_file_path}/disk2'"
ssh cs4224j@xcnd${ts1}.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-tserver --flagfile ${clusterno}ts${ts1}.conf > ${cluster_file_path}/disk1/yb-tserver.out 2>&1 &'"


ssh cs4224j@xcnd${ts2}.comp.nus.edu.sg \
"sh -c 'mkdir -p ${cluster_file_path}/disk1 ${cluster_file_path}/disk2'"
ssh cs4224j@xcnd${ts2}.comp.nus.edu.sg \
"sh -c 'cd ${yb_bin} && ./yb-tserver --flagfile ${clusterno}ts{ts2}.conf > ${cluster_file_path}/disk1/yb-tserver.out 2>&1 &'"
