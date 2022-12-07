#!/bin/ksh


bteq <<EOF
select 1
EOF
echo This is not commnted.

#while true ; do
#bteq <<EOI
#select 1;
#.IF ERRORCODE != 0 THEN
#.QUIT ERRORCODE
#.EXPORT RESET
#.LOGOFF
#.EXIT
#EOI
# echo Random stuff
#done


echo This is also not commented
