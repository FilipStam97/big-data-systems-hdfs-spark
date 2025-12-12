## For adding the converted data to hdfs

docker exec -it namenode bash

hdfs dfs -mkdir -p /data/taxi
hdfs dfs -put /opt/hadoop/project/yellow_tripdata_2016-01.parquet /data/taxi/

hdfs dfs -ls /data/taxi