WHENEVER SQLERROR EXIT 1;

-- Set SQL*Plus environment options for CSV output
SET HEADING ON;
SET FEEDBACK OFF;
SET TERMOUT OFF;
SET PAGESIZE 0;
SET LINESIZE 32767;
SET TRIMSPOOL ON;
SET COLSEP ',';

-- Accept the input file name and output file name as arguments
DEFINE INPUT_FILE = &1
DEFINE OUTPUT_FILE = &2

-- Spool the output to the specified CSV file
SPOOL &OUTPUT_FILE

-- Execute the SQL statement from the input file
@&INPUT_FILE

-- Stop spooling
SPOOL OFF;

-- Reset SQL*Plus environment options
SET FEEDBACK ON;
SET TERMOUT ON;
SET PAGESIZE 50;
SET LINESIZE 80;
SET COLSEP ' ';

-- Exit successfully
EXIT SUCCESS
