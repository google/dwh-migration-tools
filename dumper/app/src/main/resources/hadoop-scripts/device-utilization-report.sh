#!/bin/bash

if command -v iostat > /dev/null 2>&1 ; then
  iostat -d
fi
