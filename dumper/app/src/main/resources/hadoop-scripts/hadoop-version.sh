#!/bin/bash

if command -v hadoop > /dev/null 2>&1 ; then
  hadoop version
fi