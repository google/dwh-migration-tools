-- Extraction window (UTC, END_DATE inclusive). Default: last 14 days. For a custom range set e.g.
-- SET START_DATE = '2026-02-01'::DATE;  SET END_DATE = '2026-05-10'::DATE;
SET START_DATE = DATEADD('day', -14, CURRENT_DATE());
SET END_DATE = CURRENT_DATE();
WITH CreditsPerHourPerWarehouse AS (
    SELECT
        WAREHOUSE_NAME AS "WarehouseName",
        DATE_TRUNC('HOUR', START_TIME) AS "AggregateTime",
        SUM(CREDITS_USED_COMPUTE) AS "TotalCreditsBilled"
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE START_TIME >= $START_DATE::TIMESTAMP_TZ AND START_TIME < DATEADD('day', 1, $END_DATE)::TIMESTAMP_TZ
      AND WAREHOUSE_NAME IS NOT NULL
      AND WAREHOUSE_NAME != 'CLOUD_SERVICES_ONLY'
    GROUP BY 1, 2
    HAVING SUM(CREDITS_USED_COMPUTE) > 0
),
MultiObjectQueries AS (
    SELECT DISTINCT QUERY_ID
    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
    WHERE QUERY_START_TIME >= $START_DATE::TIMESTAMP_TZ AND QUERY_START_TIME < DATEADD('day', 1, $END_DATE)::TIMESTAMP_TZ
      AND ARRAY_SIZE(DIRECT_OBJECTS_ACCESSED) >= 6
),
FilteredQueryAttribution AS (
    SELECT
        QUERY_ID,
        CREDITS_ATTRIBUTED_COMPUTE
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY
    WHERE START_TIME >= $START_DATE::TIMESTAMP_TZ AND START_TIME < DATEADD('day', 1, $END_DATE)::TIMESTAMP_TZ
),
QueryRaw AS (
    SELECT
        QL.QUERY_ID,
        QL.WAREHOUSE_NAME AS "WarehouseName",
        DATE_TRUNC('HOUR', QL.START_TIME) AS "AggregateTime",
        QL.QUERY_TYPE,
        QL.USER_NAME,
        COALESCE(QL.BYTES_SCANNED, 0) AS "BytesScanned",
        COALESCE(QL.ROWS_PRODUCED, 0) AS "RowsProduced",
        GREATEST(
            CEIL(COALESCE(QL.BYTES_SCANNED, 0) / 1048576.0), 10
        ) AS "QueryBlocks",
        MOQ.QUERY_ID IS NOT NULL AS "IsMultiObject",
        QAH.CREDITS_ATTRIBUTED_COMPUTE AS "RawAttributedCredits",
        COALESCE(QL.EXECUTION_TIME, 0) AS "ExecutionTime",
        SUM(COALESCE(QL.EXECUTION_TIME, 0)) OVER (
            PARTITION BY QL.WAREHOUSE_NAME, DATE_TRUNC('HOUR', QL.START_TIME)
        ) AS "HourExecutionTimeSum",
        COUNT(*) OVER (
            PARTITION BY QL.WAREHOUSE_NAME, DATE_TRUNC('HOUR', QL.START_TIME)
        ) AS "HourQueryCount",
        COALESCE(Credits."TotalCreditsBilled", 0.0) AS "HourCreditsBilled"
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY AS QL
    LEFT JOIN CreditsPerHourPerWarehouse AS Credits
        ON QL.WAREHOUSE_NAME = Credits."WarehouseName"
       AND DATE_TRUNC('HOUR', QL.START_TIME) = Credits."AggregateTime"
    LEFT JOIN MultiObjectQueries AS MOQ
        ON QL.QUERY_ID = MOQ.QUERY_ID
    LEFT JOIN FilteredQueryAttribution AS QAH
        ON QL.QUERY_ID = QAH.QUERY_ID
    WHERE QL.WAREHOUSE_NAME IS NOT NULL
      AND QL.START_TIME >= $START_DATE::TIMESTAMP_TZ AND QL.START_TIME < DATEADD('day', 1, $END_DATE)::TIMESTAMP_TZ
),
QueryBase AS (
    SELECT
        QR.*,
        CASE
            WHEN QR."RawAttributedCredits" IS NOT NULL THEN
                QR."RawAttributedCredits"
            WHEN QR."HourExecutionTimeSum" > 0 THEN
                DIV0(QR."ExecutionTime", QR."HourExecutionTimeSum")
                    * QR."HourCreditsBilled"
            ELSE
                DIV0(1.0, QR."HourQueryCount") * QR."HourCreditsBilled"
        END AS "AttributedCredits"
    FROM QueryRaw AS QR
),
QueryClassified AS (
    SELECT
        QB.*,
        (QB.QUERY_TYPE != 'COPY' AND QB.USER_NAME != 'SYSTEM'
         AND QB."RowsProduced" > 0
         AND DIV0(QB."BytesScanned", QB."RowsProduced") >= 1073741824
        ) AS "IsFilteringAbove1Gb",
        (QB.QUERY_TYPE != 'COPY' AND QB.USER_NAME != 'SYSTEM'
         AND NOT (
             QB."RowsProduced" > 0
             AND DIV0(QB."BytesScanned", QB."RowsProduced") >= 1073741824
         )
         AND (
             (QB."BytesScanned" > 0
              AND DIV0(QB."AttributedCredits", QB."BytesScanned")
                  * 3774873600.0 < 0.003)
             OR QB."AttributedCredits" < 0.001
         )) AS "IsLowCost"
    FROM QueryBase AS QB
)
SELECT
    "WarehouseName",
    "AggregateTime",
    SUM("AttributedCredits") AS "TotalCreditsAttributed",
    SUM(IFF(
        QUERY_TYPE != 'COPY' AND USER_NAME != 'SYSTEM',
        "QueryBlocks", 0
    )) AS "TotalBlocks",
    SUM(IFF(
        QUERY_TYPE = 'COPY' AND USER_NAME != 'SYSTEM',
        "AttributedCredits", 0.0
    )) AS "CreditsForCopy",
    SUM(IFF(USER_NAME = 'SYSTEM', "AttributedCredits", 0.0))
        AS "CreditsForSystem",
    SUM(IFF("IsFilteringAbove1Gb", "AttributedCredits", 0.0))
        AS "CreditsForFilteringAbove1Gb",
    SUM(IFF("IsFilteringAbove1Gb", "QueryBlocks", 0))
        AS "BlocksForFilteringAbove1Gb",
    SUM(IFF("IsLowCost", "AttributedCredits", 0.0))
        AS "CreditsForLowCost",
    SUM(IFF("IsLowCost", "QueryBlocks", 0))
        AS "BlocksForLowCost",
    SUM(IFF(
        QUERY_TYPE != 'COPY' AND USER_NAME != 'SYSTEM'
        AND NOT "IsFilteringAbove1Gb" AND NOT "IsLowCost" AND "IsMultiObject",
        "AttributedCredits", 0.0
    )) AS "CreditsForMultiObject",
    SUM(IFF(
        QUERY_TYPE != 'COPY' AND USER_NAME != 'SYSTEM'
        AND NOT "IsFilteringAbove1Gb" AND NOT "IsLowCost" AND "IsMultiObject",
        "QueryBlocks", 0
    )) AS "BlocksForMultiObject",
    SUM(IFF(
        QUERY_TYPE != 'COPY' AND USER_NAME != 'SYSTEM'
        AND NOT "IsFilteringAbove1Gb" AND NOT "IsLowCost" AND NOT "IsMultiObject",
        "AttributedCredits", 0.0
    )) AS "CreditsForStandard",
    SUM(IFF(
        QUERY_TYPE != 'COPY' AND USER_NAME != 'SYSTEM'
        AND NOT "IsFilteringAbove1Gb" AND NOT "IsLowCost" AND NOT "IsMultiObject",
        "QueryBlocks", 0
    )) AS "BlocksForStandard"
FROM QueryClassified
GROUP BY 1, 2
HAVING SUM("AttributedCredits") <> 0
    OR SUM(IFF(QUERY_TYPE != 'COPY' AND USER_NAME != 'SYSTEM', "QueryBlocks", 0)) <> 0
ORDER BY "WarehouseName" ASC, "AggregateTime" ASC;
