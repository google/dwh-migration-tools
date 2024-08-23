#!/bin/bash

if command -v spark-submit > /dev/null 2>&1 ; then
  spark-submit --version
fi
