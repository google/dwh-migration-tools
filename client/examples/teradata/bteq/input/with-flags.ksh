#!/bin/ksh

##############################################################################
function main
{

FOO=QUX_${BAR}_BAZ

if [[ -f ${OUTDIR}/file.tmp ]];then
        rm -f ${OUTDIR}/file.tmp
fi

 bteq <<-EOF1
 
 .LOGON ${LOGON_STRING};
 .export data file=${OUTDIR}/file.tmp;
 select trim(((date - ${DAYIND}(integer)) (integer))(char(20)))(char(7));
 .export reset;
 .quit errorcode;
EOF1
 RETURN_CD=$?
 if [ $RETURN_CD != 0 ]
 then
         return $RETURN_CD
 fi
 
BDATE=`cut -c3-9 ${OUTDIR}/file.tmp`

bteq <<-EOF

.LOGON ${LOGON_STRING};
select 'uninteresting-lot-of-sql';
.if errorcode != 0 then .quit errorcode
.quit errorcode;
EOF
ERRCD=$?
if [ $ERRCD != 0 ]
        then return 99
fi

return $?

}

main > $lf 2>&1

