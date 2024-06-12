#!/bin/bash

if command -v sqoop > /dev/null 2>&1 ; then
  sqoop version
fi
