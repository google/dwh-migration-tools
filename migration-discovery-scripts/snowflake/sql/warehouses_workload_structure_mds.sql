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
        QL.WAREHOUSE_SIZE,
        DATE_TRUNC('HOUR', QL.START_TIME) AS "AggregateTime",
        QL.QUERY_TYPE,
        COALESCE(QL.BYTES_SCANNED, 0) AS "BytesScanned",
        COALESCE(QL.ROWS_PRODUCED, 0) AS "RowsProduced",
        GREATEST(
            CEIL(COALESCE(QL.BYTES_SCANNED, 0) / 1048576.0), 10
        ) AS "QueryBlocks",
        MOQ.QUERY_ID IS NOT NULL AS "IsMultiObject",
        QAH.CREDITS_ATTRIBUTED_COMPUTE AS "RawAttributedCredits",
        COALESCE(
            SUM(QAH.CREDITS_ATTRIBUTED_COMPUTE) OVER (
                PARTITION BY QL.WAREHOUSE_NAME, DATE_TRUNC('HOUR', QL.START_TIME)
            ),
            0.0
        ) AS "HourAttributedCreditsSum",
        GREATEST(COALESCE(QL.EXECUTION_TIME, 0), 1) AS "WeightedExecutionTime",
        SUM(GREATEST(COALESCE(QL.EXECUTION_TIME, 0), 1)) OVER (
            PARTITION BY QL.WAREHOUSE_NAME, DATE_TRUNC('HOUR', QL.START_TIME)
        ) AS "HourExecutionTimeSum",
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
      AND QL.WAREHOUSE_SIZE IS NOT NULL
      AND QL.START_TIME >= $START_DATE::TIMESTAMP_TZ AND QL.START_TIME < DATEADD('day', 1, $END_DATE)::TIMESTAMP_TZ
),
QueryBase AS (
    SELECT
        QR.*,
        IFF(
            QR."HourAttributedCreditsSum" > 0,
            QR."RawAttributedCredits",
            DIV0(QR."WeightedExecutionTime", QR."HourExecutionTimeSum")
                * QR."HourCreditsBilled"
        ) AS "AttributedCredits"
    FROM QueryRaw AS QR
),
QueryClassified AS (
    SELECT
        QB.*,
        (QB.QUERY_TYPE != 'COPY'
         AND DIV0(QB."BytesScanned", GREATEST(QB."RowsProduced", 1)) >= 1073741824
        ) AS "IsFilteringAbove1Gb",
        (QB.QUERY_TYPE != 'COPY'
         AND NOT (
             DIV0(QB."BytesScanned", GREATEST(QB."RowsProduced", 1)) >= 1073741824
         )
         AND (
             (QB."BytesScanned" > 0
              AND DIV0(QB."AttributedCredits", QB."BytesScanned")
                  * 3774873600.0 < 0.003)
             OR QB."AttributedCredits" IS NULL
             OR QB."AttributedCredits" < 0.001
         )) AS "IsLowCost"
    FROM QueryBase AS QB
)
SELECT
    "WarehouseName",
    "AggregateTime",
    BOOLOR_AGG(UPPER(WAREHOUSE_SIZE) = 'ADAPTIVE') AS "IsAdaptiveWarehouse",
    SUM(COALESCE("AttributedCredits", 0.0)) AS "TotalCreditsAttributed",
    SUM(IFF(
        QUERY_TYPE != 'COPY',
        "QueryBlocks", 0
    )) AS "TotalBlocks",
    SUM(IFF(
        QUERY_TYPE = 'COPY',
        COALESCE("AttributedCredits", 0.0), 0.0
    )) AS "CreditsForCopy",
    0.0 AS "CreditsForSystem",
    SUM(IFF("IsFilteringAbove1Gb", COALESCE("AttributedCredits", 0.0), 0.0))
        AS "CreditsForFilteringAbove1Gb",
    SUM(IFF("IsFilteringAbove1Gb", "QueryBlocks", 0))
        AS "BlocksForFilteringAbove1Gb",
    SUM(IFF("IsLowCost", COALESCE("AttributedCredits", 0.0), 0.0))
        AS "CreditsForLowCost",
    SUM(IFF("IsLowCost", "QueryBlocks", 0))
        AS "BlocksForLowCost",
    SUM(IFF(
        QUERY_TYPE != 'COPY'
        AND NOT "IsFilteringAbove1Gb" AND NOT "IsLowCost" AND "IsMultiObject",
        COALESCE("AttributedCredits", 0.0), 0.0
    )) AS "CreditsForMultiObject",
    SUM(IFF(
        QUERY_TYPE != 'COPY'
        AND NOT "IsFilteringAbove1Gb" AND NOT "IsLowCost" AND "IsMultiObject",
        "QueryBlocks", 0
    )) AS "BlocksForMultiObject",
    SUM(IFF(
        QUERY_TYPE != 'COPY'
        AND NOT "IsFilteringAbove1Gb" AND NOT "IsLowCost" AND NOT "IsMultiObject",
        COALESCE("AttributedCredits", 0.0), 0.0
    )) AS "CreditsForStandard",
    SUM(IFF(
        QUERY_TYPE != 'COPY'
        AND NOT "IsFilteringAbove1Gb" AND NOT "IsLowCost" AND NOT "IsMultiObject",
        "QueryBlocks", 0
    )) AS "BlocksForStandard"
FROM QueryClassified
GROUP BY 1, 2
HAVING SUM(COALESCE("AttributedCredits", 0.0)) <> 0
    OR SUM(IFF(QUERY_TYPE != 'COPY', "QueryBlocks", 0)) <> 0
ORDER BY "WarehouseName" ASC, "AggregateTime" ASC;
