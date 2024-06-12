#!/bin/bash

if command -v spark-shell > /dev/null 2>&1 ; then
  spark-shell version
fi