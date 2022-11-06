# 2022fall-cs5424-distributed-database-project

## 0. Connect to NUS SOC VPN

In order to visit NUS SOC intranet, connecting to SOC VPN is required.

To setup, consider this [SOC VPN Setup Guide](https://dochub.comp.nus.edu.sg/cf/guides/network/vpn)

## 1. SSH Public Key Authentication to 5 Servers

As required, we use `ssh` to access the 5 SoC servers. To avoid entering password repeatedly when simultaniously sending instructions to servers every time, please configure the **SSH public key authentication** to the 5 servers using the following steps.

<!--改写自邓哥-->

To obtain ssh connection to 5 servers, on your **local machine**, enter `~/.ssh`:

```bash
# build a new encryption key pair
cd ~/.ssh && ssh-keygen
# keep typing 'enter' until the key pair is generated
```

Given user `cs4224j`'s password `+rW8FLp3`, now copy the generated ssh public key into each server by the below command and repeat this command **for xcnd21 to xcnd 24**.

```bash
ssh-copy-id cs4224j@xcnd20.comp.nus.edu.sg
# and type 'yes' or the user password `+rW8FLp3` when see according messages
```

Now you wil not enter password repeatedly when use ssh to connect any server.

Note: **~** in **any xcndXX servers**, referring to `/home/stuproj/cs4224j` will frequently be use as our **default user path** in below sections.

## 2. Setup Files(at the cluster already)

- The data and xact raw files are already located in `~/project_data/data_files`(including our modified data) and `~/project_data/xact_files`.
- The database initialization files, in <u>.sql</u> and <u>.cql</u> are already located in `~/db_init_csql_SoCluster`.

### Install YugabyteDB

- The yugabyteDB setup is already installed in `~/yugabyte-2.14.1.0`.
  - To install from the scratch, its original installer file, _yugabyte-2.14.1.0-b36-linux-x86_64.tar.gz_ is at ~ which can be decompressed and installed into the final software`.

```bash
# Download the YugabyteDB binary package and decompress
tar xvfz yugabyte-2.14.1.0-b36-linux-x86_64.tar.gz && cd yugabyte-2.14.1.0/

# Configure by post installation
./bin/post_install.sh
```

## 3. Cluster-initialization(in this repo)

### Cluster-initialization scripts

There are several cluster-related <u>bash scripts</u> for different use case located **in our repositoy** path `gj2-ram/`. First you should change directory into the script path by `cd gj2-ram`:

- The **5-server yugabyte cluster, with all databases, intialized in just <u>one</u> step** can be newly built from the scratch with the command `./build_new_gj2cluster.sh`. Then to check cluster status, you can:

  1. go to each server by `ssh` to check if the yb-master and yb-tserver processes are running by the command `ps -eaf | grep yb`, or

  2. check the yugabyte web server monitoring UI in your **browser** at the url `http://xcnd21.comp.nus.edu.sg:7300/` (if doesn't work, replace `xncd21` in the url to any other serverID among [xncd20, xncd22, xncd23, xncd24] ). If all servers are smoothly started, the number of nodes in the UI should be 5 as below.

     ![cluster monitoring UI](https://tva1.sinaimg.cn/large/008vxvgGgy1h7u63xyb0aj31k90u0gp1.jpg)

- Also you can initialize a 5-server cluster with all databases step by step, try below steps separately:

  1.  if you want to only start cluster first, then run `./start_cluster_gj2.sh`. For the status check, go as mentioned just above.

  1.  If some server fails to start a ybmaster/tserver process, you have to start it at the node(s) **manually**:

      ```bash
      # On xcnd20.comp.nus.edu.sg
      ssh cs4224j@xcnd20.comp.nus.edu.sg
      # and/or start a ybmaster
      cd ~/yugabyte-2.14.1.0/bin && ./yb-master --flagfile 2ms20.conf >& /mnt/ramdisk/gj2/disk1/yb-master.out &
      # and/or start a tserver
      cd ~/yugabyte-2.14.1.0/bin && ./yb-tserver --flagfile 2ts20.conf >& /mnt/ramdisk/gj2/disk1/yb-tserver.out &

      # On xcnd21.comp.nus.edu.sg
      ssh cs4224j@xcnd21.comp.nus.edu.sg
      # and/or start a ybmaster
      cd ~/yugabyte-2.14.1.0/bin && ./yb-master --flagfile 2ms21.conf >& /mnt/ramdisk/gj2/disk1/yb-master.out &
      # and/or start a tserver
      cd ~/yugabyte-2.14.1.0/bin && ./yb-tserver --flagfile 2ts21.conf >& /mnt/ramdisk/gj2/disk1/yb-tserver.out &

      # On xcnd22.comp.nus.edu.sg
      ssh cs4224j@xcnd22.comp.nus.edu.sg
      # and/or start a ybmaster
      cd ~/yugabyte-2.14.1.0/bin && ./yb-master --flagfile 2ms22.conf >& /mnt/ramdisk/gj2/disk1/yb-master.out &
      # and/or start a tserver
      cd ~/yugabyte-2.14.1.0/bin && ./yb-tserver --flagfile 2ts22.conf >& /mnt/ramdisk/gj2/disk1/yb-tserver.out &

      # On xcnd23.comp.nus.edu.sg
      ssh cs4224j@xcnd23.comp.nus.edu.sg
      # and/or start a ybmaster
      cd ~/yugabyte-2.14.1.0/bin && ./yb-master --flagfile 2ms23.conf >& /mnt/ramdisk/gj2/disk1/yb-master.out &
      # and/or start a tserver
      cd ~/yugabyte-2.14.1.0/bin && ./yb-tserver --flagfile 2ts23.conf >& /mnt/ramdisk/gj2/disk1/yb-tserver.out &

      ssh cs4224j@xcnd24.comp.nus.edu.sg
      # and/or start a ybmaster
      cd ~/yugabyte-2.14.1.0/bin && ./yb-master --flagfile 2ms24.conf >& /mnt/ramdisk/gj2/disk1/yb-master.out &
      # and/or start a tserver
      cd ~/yugabyte-2.14.1.0/bin && ./yb-tserver --flagfile 2ts24.conf >& /mnt/ramdisk/gj2/disk1/yb-tserver.out &
      ```

  1. If the cluster has already started with at least 3 master servers and **at least 3 tservers** (go into the **Tablet Servers** on the left to see how many tservers are alive as below),

     ![Tablet Server Status](https://tva1.sinaimg.cn/large/008vxvgGgy1h7u7008yafj31b70u0gpq.jpg)

     then you can then build database by `./initcql_gj2.sh` for a YCQL database and `./initsql_gj2.sh` for a YSQL database. The console will show processes of importing each table/keyspace like below.

     

  1.  When to the end of usage, to terminate the cluster, try `./end_cluster_gj2.sh`.

- To delete a cluster completely by removing all the cluster-related files, try `./rm_files_gj2.sh`.

## Experiment Files(in this repo)

TODO 阳哥介绍下 java 的运行程序文件放在 repo 的哪, **分别干啥的 怎么运行 **

#### Compile `cassandra.jar` and `cockroachdb.jar`

```zsh
$ mvn clean package
```

### Instructions for running experiments

### YSQL

1. Run experiments for workload A/B with 40 clients

   ```zsh
   $ ./scripts/run_experiments.sh <experiment-number> <workload-type> cockroachdb 26267
   ```

   e.g.

   ```zsh
   $ ./scripts/run_experiments.sh 1 A cockroachdb 26267
   ```

   Outputs for each client will be stored at `out/cockroachdb-<experiment-number>-<workload-type>-<client-id>.out`

   Final statistics will be stored at `out/cockroachdb-<experiment-number>-<workload-type>.csv`

   If you only need to run one client:

   ```zsh
   $ java -jar target/cockroachdb.jar <hostname> 26267 <workload-type> <client-id> <statistics-csv-dir> 0
   ```

   e.g.

   ```zsh
   $ java -jar target/cockroachdb.jar xcnd30.comp.nus.edu.sg 26267 A a clients.csv 0
   ```

3. To abort experiments
   ```
   $ ./scripts/stop_experiments.sh
   ```

### YCQL

1. Run experiments for workload A/B with 40 clients

   ```zsh
   $ ./scripts/run_experiments.sh <experiment-number> <workload-type> cassandra 3042
   ```

   e.g.

   ```zsh
   $ ./scripts/run_experiments.sh 1 A cassandra 3042
   ```

   Outputs for each client will be stored at `out/cassandra-<experiment-number>-<workload-type>-<client-id>.out`

   Final statistics will be stored at `out/cassandra-<experiment-number>-<workload-type>.csv`

   If you only need to run one client:

   ```zsh
   $ java -jar target/cassandra.jar <hostname> 3042 <workload-type> <client-id> <statistics-csv-dir> 0
   ```

   e.g.

   ```zsh
   $ java -jar target/cassandra.jar xcnd30.comp.nus.edu.sg 3042 A a clients.csv 0
   ```

4. To abort experiments
   ```
   $ ./scripts/stop_experiments.sh
   ```



## Cluster Clean-up

- When to the end of usage, to terminate the cluster, try `./end_cluster_gj2.sh`.

- To delete a cluster completely by removing all the cluster-related files, try `./rm_files_gj2.sh`.
