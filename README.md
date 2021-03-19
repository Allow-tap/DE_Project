# Data Engineering Group Project UU Spring 2021 Team 9

# Spark Cluster & hdfs Setup 
ubuntu 18.04

/etc/hosts on both machines
192.168.2.207 spark-master
192.168.2.210 spark-slave

Test that you can ping spark-master/slave

## BOTH
sudo apt-get update
sudo apt-get install openjdk-11-jdk
sudo apt-get install software-properties-common
sudo apt-get install scala

## MASTER
ssh-keygen -t rsa -P ""
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
ssh-copy-id ubuntu@spark-slave
Add this key to authorized_keys in slave

## BOTH
wget https://ftp.acc.umu.se/mirror/apache.org/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz
tar -xvf spark-3.1.1-bin-hadoop2.7.tgz
sudo mv spark-3.1.1-bin-hadoop2.7/ /usr/local/spark
Add export PATH=$PATH:/usr/local/spark/bin to bashrc and source bashrc
and also add this export HADOOP_HOME='/home/ubuntu/hadoop-3.3.0' to bashrc and hadoop-env.sh
hadoop-env.sh: export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native

## MASTER
cd /usr/local/spark
cp conf/spark-env.sh.template conf/spark-env.sh

edit conf/spark-env.sh
export SPARK_MASTER_HOST='192.168.2.207'
export JAVA_HOME='/usr/lib/jvm/java-11-openjdk-amd64'

edit conf/slaves
spark-master
spark-slave

./sbin/start-all.sh
jps - check what's running

also jps on worker to check
## BOTH (HADOOP START)
sudo apt-get install rsync
cd ~
wget https://ftpmirror1.infania.net/mirror/apache/hadoop/common/hadoop-3.3.0/hadoop-3.3.0.tar.gz
tar -xf hadoop-3.3.0.tar.gz

* in hadoop-home/etc/hadoop/core-site.xml
<configuration>
        <property>
                <name>fs.default.name</name>
                <value>hdfs://spark-master:9000</value>
        </property>
</configuration>

* in hadoop-home/etc/hadoop/hdfs-site.xml
     <property>
         <name>dfs.replication</name>
         <value>1</value>
     </property>

in hadoop-home/etc/hadoop/mapred-site.xml
<configuration>
     <property>
         <name>mapred.job.tracker</name>
         <value>spark-master:9001</value>
     </property>
</configuration>

in hadoop-home/etc/hadoop/hadoop-env.sh
export HADOOP_HOME='/home/ubuntu/hadoop-3.3.0'
export JAVA_HOME='/usr/lib/jvm/java-11-openjdk-amd64'
export HADOOP_CONF_DIR='/home/ubuntu/hadoop-3.3.0/etc/hadoop'
## MASTER
in etc/hadoop/slaves
spark-master
spark-slave


bin/hadoop namenode -format
sbin/start-dfs.sh
## WORKER
sbin/hadoop-daemon.sh start datanode
## MASTER
bin/hdfs dfs -mkdir /user
bin/hdfs dfs -mkdir /user/ubuntu
bin/hdfs dfs -put sample_data.json /user/ubuntu/

## Driver Info
echo "export PYSPARK_PYTHON=python3" >> ~/.bashrc
sudo apt-get update
sudo apt-get install openjdk-11-jdk
source ~/.bashrc
sudo apt-get install -y python3-pip
python3 -m pip install pyspark==3.1.1 --user --no-cache-dir
python3 -m pip install pandas --user
python3 -m pip install matplotlib --user
python3 -m pip install jupyterlab
python3 -m jupyterlab
