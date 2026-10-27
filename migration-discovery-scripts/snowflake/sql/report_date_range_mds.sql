-- Extraction window (UTC, END_DATE inclusive). Default: last 14 days. For a custom range set e.g.
-- SET START_DATE = '2026-02-01'::DATE;  SET END_DATE = '2026-05-10'::DATE;
SET START_DATE = DATEADD('day', -14, CURRENT_DATE());
SET END_DATE = CURRENT_DATE();
SELECT
    $START_DATE::TIMESTAMP_TZ AS "StartTime",
    DATEADD('day', 1, $END_DATE)::TIMESTAMP_TZ AS "EndTime";

