#!/bin/bash

if command -v hbase > /dev/null 2>&1 ; then
  hbase version
fi
