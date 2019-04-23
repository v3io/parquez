#!/usr/bin/bash -x

/hive/bin/hive -hiveconf hive.metastore.uris=thrift://hive:9083 -f tableName.txt


