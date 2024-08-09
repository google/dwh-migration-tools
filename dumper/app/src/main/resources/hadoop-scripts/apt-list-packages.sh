#!/bin/bash

if command -v apt > /dev/null 2>&1 ; then
  apt list --installed | grep "^\(docker\|g++\|gcc\|hadoop\|hbase\|hive\|mysql\|oozie\|perl\|pig\|python\|r-\|ranger\|ruby\|scala\|spark\|sqoop\|tez/\|zookeeper\)"
fi
