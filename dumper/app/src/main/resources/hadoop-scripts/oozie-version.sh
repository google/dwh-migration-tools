#!/bin/bash

if command -v oozie > /dev/null 2>&1 ; then
  oozie version
fi
