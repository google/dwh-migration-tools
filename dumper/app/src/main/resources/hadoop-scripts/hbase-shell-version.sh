#!/bin/bash

if command -v hbase > /dev/null 2>&1 ; then
  echo -n status | hbase shell -n
fi
