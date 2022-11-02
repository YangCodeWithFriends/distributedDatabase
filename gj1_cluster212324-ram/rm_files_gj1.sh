#!/bin/bash
###
 # @Author: YuhaoWU
 # @Date: 2022-11-01 11:37:30
 # @LastEditors: YuhaoWU
 # @LastEditTime: 2022-11-03 00:29:21
 # @Description: 
### 


# yb_bin=/home/stuproj/cs4224j/yugabyte-2.14.1.0/bin
cluster_file_path=/mnt/ramdisk/gj1

# 先stop 集群
${bashCurPath}/end_cluster_gj1.sh

ssh cs4224j@xcnd20.comp.nus.edu.sg \
"sh -c 'rm -rf ${cluster_file_path}/*'"

ssh cs4224j@xcnd21.comp.nus.edu.sg \
"sh -c 'rm -rf ${cluster_file_path}/*'"

ssh cs4224j@xcnd22.comp.nus.edu.sg \
"sh -c 'rm -rf ${cluster_file_path}/*'"

ssh cs4224j@xcnd23.comp.nus.edu.sg \
"sh -c 'rm -rf ${cluster_file_path}/*  && ls ${cluster_file_path}/'"

ssh cs4224j@xcnd24.comp.nus.edu.sg \
"sh -c 'rm -rf ${cluster_file_path}/*  && ls ${cluster_file_path}/'"
