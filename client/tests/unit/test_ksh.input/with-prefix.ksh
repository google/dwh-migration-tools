#!/bin/ksh

SCRIPTS_DIR=$HOME/scripts/Teradata_ETL/
TABLE_NAME=

$HOME/scripts/Teradata/bteq << EOF > $SCRIPTS_DIR/logs/`basename $0`.log 2>&1
.maxerror 1;
.run file = $SCRIPTS_DIR/ETL_USER_$test_environment.login;

delete stg.foo;
;insert into stg.foo
SELECT     A.foo, A.bar, A.baz,
    cast(AVG(b.quam) as decimal) as quix,
    cast(avg(cast(A.qib as date) - cast(A.zim as date)) as decimal) as zob,
    SUM(A.jibber) as jabber
FROM 
    tb.foo a,sys_calendar.calendar b 
WHERE 
    a.qib = b.quam
    AND A.zif = 1
    AND A.zaf = 1
    AND A.cor >= (select year_of_calendar-1||'0101' from sys_calendar.calendar 

where calendar_date = current_date)
    AND A.jif = 1		
    AND A.foo IS NOT NULL
    AND A.bar IS NOT NULL
    AND A.quam IS NOT NULL
GROUP BY  A.foo,A.bar,A.baz;
.quit;
EOF

exit $?
