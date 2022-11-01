#!/bin/bash
###
 # @Author: YuhaoWU
 # @Date: 2022-11-01 12:00:53
 # @LastEditors: YuhaoWU
 # @LastEditTime: 2022-11-01 12:42:26
 # @Description: 
### 


# yb_bin=/home/stuproj/cs4224j/yugabyte-2.14.1.0/bin
cluster_file_path=/temp/gj3/


ssh cs4224j@xcnd20.comp.nus.edu.sg \
"sh -c 'rm -rf ${cluster_file_path}/*'"

ssh cs4224j@xcnd21.comp.nus.edu.sg \
"sh -c 'rm -rf ${cluster_file_path}/*'"


ssh cs4224j@xcnd22.comp.nus.edu.sg \
"sh -c 'rm -rf ${cluster_file_path}/*'"

ssh cs4224j@xcnd23.comp.nus.edu.sg \
"sh -c 'rm -rf ${cluster_file_path}/* && ls ${cluster_file_path}/'"

ssh cs4224j@xcnd24.comp.nus.edu.sg \
"sh -c 'rm -rf ${cluster_file_path}/* && ls ${cluster_file_path}/'"

