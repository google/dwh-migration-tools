#!/bin/ksh

## A Very simple test.


bteq  <<EOF
SELECT
    1
;
select   '$Something' ;
select   '`backtick`';
select   '\\esc';

EOF

echo Trying another select.

bteq  << "EOF"
SELECT
    2
;
select   '$Something' ;
select   '`backtick`';
select   '\\esc';

EOF


echo Quotes are meaningful

bteq  <<'EOF'
select   '$Something' ;
select   '`backtick`';
select   '\\esc';

EOF


