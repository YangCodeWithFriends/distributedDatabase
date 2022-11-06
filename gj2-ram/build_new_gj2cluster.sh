
###
 # @Author: YuhaoWU
 # @Date: 2022-11-01 23:51:50
 # @LastEditors: YuhaoWU
 # @LastEditTime: 2022-11-04 10:44:35
 # @Description: 启动全新的cluster, 包括删除之前的 + 导入新数据
### 


bashCurPath=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "Your local script path is: $bashCurPath"


# 停掉如果还在run的集群
${bashCurPath}/end_cluster_gj2.sh

# 删除之前的
${bashCurPath}/rm_files_gj2.sh

# create and start new cluster
${bashCurPath}/start_cluster_gj2.sh 243

# tmux
${bashCurPath}/initsql_gj2_parti.sh 241
# ${bashCurPath}/initcql_gj2noidx.sh 241

# import data
${bashCurPath}/initsql_gj2.sh 243

