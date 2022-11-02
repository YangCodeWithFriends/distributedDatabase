
###
 # @Author: YuhaoWU
 # @Date: 2022-11-01 23:51:50
 # @LastEditors: YuhaoWU
 # @LastEditTime: 2022-11-02 12:10:41
 # @Description: 启动全新的cluster, 包括删除之前的 + 导入新数据
### 


bashCurPath=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "Your local script path is: $bashCurPath"

# 停掉如果还在run的集群
${bashCurPath}/end_cluster_gj4.sh

# # 删除之前的
${bashCurPath}/rm_files_gj4.sh

# # create and start new cluster
${bashCurPath}/start_cluster_gj4.sh

# # import data
${bashCurPath}/initsql_gj4.sh 242

${bashCurPath}/initcql_gj4.sh 241

