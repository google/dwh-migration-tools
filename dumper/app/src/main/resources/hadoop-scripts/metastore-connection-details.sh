#!/bin/bash

if command -v beeline > /dev/null 2>&1 ; then
  beeline -u jdbc:hive2:// -e "set javax.jdo.option.ConnectionURL"
fi
