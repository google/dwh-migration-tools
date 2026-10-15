SET LOOKBACK_DAYS = 14;
WITH Multipliers AS (
    SELECT
        10.0 AS HeavyScansMultiplier,
        110.0 AS TinyQueriesMultiplier,
        45.0 AS ComplexJoinsMultiplier,
        18.0 AS OtherMultiplier
),
CreditsPerHourPerWarehouse AS (
    SELECT
        WAREHOUSE_NAME AS "WarehouseName",
        DATE_TRUNC('HOUR', START_TIME) AS "AggregateTime",
        SUM(CREDITS_USED_COMPUTE) AS "TotalCreditsBilled"
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE START_TIME >= DATEADD('day', -$LOOKBACK_DAYS, CURRENT_TIMESTAMP())
      AND WAREHOUSE_NAME IS NOT NULL
    GROUP BY 1, 2
),
ComplexJoinsFromHistory AS (
    SELECT DISTINCT QUERY_ID
    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
    WHERE QUERY_START_TIME >= DATEADD('day', -$LOOKBACK_DAYS, CURRENT_TIMESTAMP())
      AND ARRAY_SIZE(DIRECT_OBJECTS_ACCESSED) >= 6
),
FilteredQueryAttribution AS (
    SELECT
        QUERY_ID,
        COALESCE(CREDITS_ATTRIBUTED_COMPUTE, 0.0) AS CREDITS_ATTRIBUTED_COMPUTE
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY
    WHERE START_TIME >= DATEADD('day', -$LOOKBACK_DAYS, CURRENT_TIMESTAMP())
),
QueryRaw AS (
    SELECT
        QL.QUERY_ID,
        QL.WAREHOUSE_NAME AS "WarehouseName",
        DATE_TRUNC('HOUR', QL.START_TIME) AS "AggregateTime",
        CASE UPPER(COALESCE(QL.WAREHOUSE_SIZE, 'SMALL'))
            WHEN 'X-SMALL' THEN 1
            WHEN 'SMALL' THEN 2
            WHEN 'MEDIUM' THEN 4
            WHEN 'LARGE' THEN 8
            WHEN 'X-LARGE' THEN 16
            WHEN '2X-LARGE' THEN 32
            WHEN '3X-LARGE' THEN 64
            WHEN '4X-LARGE' THEN 128
            WHEN '5X-LARGE' THEN 256
            WHEN '6X-LARGE' THEN 512
            ELSE 2
        END AS "NodeCount",
        QL.CLUSTER_NUMBER,
        COALESCE(QL.QUEUED_OVERLOAD_TIME, 0) AS QUEUED_OVERLOAD_TIME,
        QL.QUERY_TYPE,
        QL.USER_NAME,
        COALESCE(QL.BYTES_SCANNED, 0) AS "BytesScanned",
        COALESCE(QL.ROWS_PRODUCED, 0) AS "RowsProduced",
        GREATEST(CEIL(COALESCE(QL.BYTES_SCANNED, 0) / 1048576.0), 10) AS "QueryBlocks",
        CQ.QUERY_ID IS NOT NULL AS "HasComplexJoins",
        COALESCE(QAH.CREDITS_ATTRIBUTED_COMPUTE, 0.0) AS "RawAttributedCredits",
        SUM(COALESCE(QAH.CREDITS_ATTRIBUTED_COMPUTE, 0.0)) OVER (
            PARTITION BY QL.WAREHOUSE_NAME, DATE_TRUNC('HOUR', QL.START_TIME)
        ) AS "HourAttributedCreditsSum",
        COALESCE(QL.EXECUTION_TIME, 0) AS "ExecutionTime",
        SUM(COALESCE(QL.EXECUTION_TIME, 0)) OVER (
            PARTITION BY QL.WAREHOUSE_NAME, DATE_TRUNC('HOUR', QL.START_TIME)
        ) AS "HourExecutionTimeSum",
        COUNT(*) OVER (
            PARTITION BY QL.WAREHOUSE_NAME, DATE_TRUNC('HOUR', QL.START_TIME)
        ) AS "HourQueryCount",
        COALESCE(Credits."TotalCreditsBilled", 1.0) AS "HourCreditsBilled"
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY AS QL
    LEFT JOIN CreditsPerHourPerWarehouse AS Credits
        ON QL.WAREHOUSE_NAME = Credits."WarehouseName"
       AND DATE_TRUNC('HOUR', QL.START_TIME) = Credits."AggregateTime"
    LEFT JOIN ComplexJoinsFromHistory AS CQ
        ON QL.QUERY_ID = CQ.QUERY_ID
    LEFT JOIN FilteredQueryAttribution AS QAH
        ON QL.QUERY_ID = QAH.QUERY_ID
    WHERE QL.WAREHOUSE_NAME IS NOT NULL
      AND QL.START_TIME >= DATEADD('day', -$LOOKBACK_DAYS, CURRENT_TIMESTAMP())
),
QueryBase AS (
    SELECT
        QR.*,
        CASE
            WHEN QR."HourAttributedCreditsSum" > 0 THEN QR."RawAttributedCredits"
            WHEN QR."HourExecutionTimeSum" > 0 THEN
                DIV0(QR."ExecutionTime", QR."HourExecutionTimeSum") * QR."HourCreditsBilled"
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
        ) AS "IsHeavyScan",
        (QB.QUERY_TYPE != 'COPY' AND QB.USER_NAME != 'SYSTEM'
         AND NOT (
             QB."RowsProduced" > 0
             AND DIV0(QB."BytesScanned", QB."RowsProduced") >= 1073741824
         )
         AND (
             (QB."BytesScanned" > 0
              AND DIV0(QB."AttributedCredits", QB."BytesScanned") * 3774873600.0 < 0.003)
             OR QB."AttributedCredits" < 0.001
         )) AS "IsTinyQuery"
    FROM QueryBase AS QB
),
QueryAnalysisPerWarehousePerHour AS (
    SELECT
        "WarehouseName",
        "AggregateTime",
        MAX("NodeCount") AS "NodesCount",
        COUNT(*) AS "QueryCount",
        COUNT(DISTINCT CLUSTER_NUMBER) AS "ActiveClusters",
        SUM(QUEUED_OVERLOAD_TIME) / 1000.0 AS "QueuedOverloadSeconds",
        COUNT_IF(QUEUED_OVERLOAD_TIME > 0) AS "QueuedQueries",
        SUM("AttributedCredits") AS "TotalCreditsAttributed",
        SUM(IFF(QUERY_TYPE != 'COPY' AND USER_NAME != 'SYSTEM', "QueryBlocks", 0)) AS "TotalBlocks",
        SUM(IFF(QUERY_TYPE = 'COPY' AND USER_NAME != 'SYSTEM', "AttributedCredits", 0.0)) AS "CreditsForIngestion",
        SUM(IFF(USER_NAME = 'SYSTEM', "AttributedCredits", 0.0)) AS "WastedCredits",
        SUM(IFF("IsHeavyScan", "AttributedCredits", 0.0)) AS "CreditsForHeavyScans",
        SUM(IFF("IsHeavyScan", "QueryBlocks", 0)) AS "BlocksForHeavyScans",
        SUM(IFF("IsTinyQuery", "AttributedCredits", 0.0)) AS "CreditsForTinyQueries",
        SUM(IFF("IsTinyQuery", "QueryBlocks", 0)) AS "BlocksForTinyQueries",
        SUM(IFF(
            QUERY_TYPE != 'COPY' AND USER_NAME != 'SYSTEM'
            AND NOT "IsHeavyScan" AND NOT "IsTinyQuery" AND "HasComplexJoins",
            "AttributedCredits", 0.0
        )) AS "CreditsForComplexJoins",
        SUM(IFF(
            QUERY_TYPE != 'COPY' AND USER_NAME != 'SYSTEM'
            AND NOT "IsHeavyScan" AND NOT "IsTinyQuery" AND "HasComplexJoins",
            "QueryBlocks", 0
        )) AS "BlocksForComplexJoins",
        SUM(IFF(
            QUERY_TYPE != 'COPY' AND USER_NAME != 'SYSTEM'
            AND NOT "IsHeavyScan" AND NOT "IsTinyQuery" AND NOT "HasComplexJoins",
            "AttributedCredits", 0.0
        )) AS "CreditsForOther",
        SUM(IFF(
            QUERY_TYPE != 'COPY' AND USER_NAME != 'SYSTEM'
            AND NOT "IsHeavyScan" AND NOT "IsTinyQuery" AND NOT "HasComplexJoins",
            "QueryBlocks", 0
        )) AS "BlocksForOther"
    FROM QueryClassified
    GROUP BY 1, 2
),
StructureCalculated AS (
    SELECT
        Credits."WarehouseName",
        Credits."AggregateTime",
        COALESCE(Queries."NodesCount", 1) AS "NodesCount",
        COALESCE(Queries."QueryCount", 0) AS "QueryCount",
        COALESCE(Queries."ActiveClusters", 0) AS "ActiveClusters",
        COALESCE(Queries."QueuedOverloadSeconds", 0.0) AS "QueuedOverloadSeconds",
        COALESCE(Queries."QueuedQueries", 0) AS "QueuedQueries",
        COALESCE(Credits."TotalCreditsBilled", 0.0) AS "TotalCreditsBilled",
        COALESCE(Queries."TotalCreditsAttributed", 0.0) AS "TotalCreditsAttributed",
        COALESCE(Queries."WastedCredits", 0.0) AS "WastedCredits",
        COALESCE(Queries."TotalBlocks", 0) AS "TotalBlocks",
        COALESCE(Queries."BlocksForHeavyScans", 0) AS "BlocksForHeavyScans",
        COALESCE(Queries."BlocksForTinyQueries", 0) AS "BlocksForTinyQueries",
        COALESCE(Queries."BlocksForComplexJoins", 0) AS "BlocksForComplexJoins",
        COALESCE(Queries."BlocksForOther", 0) AS "BlocksForOther",
        COALESCE(DIV0(Queries."CreditsForHeavyScans", Queries."TotalCreditsAttributed"), 0.0) AS "HeavyScansPercent",
        COALESCE(DIV0(Queries."CreditsForTinyQueries", Queries."TotalCreditsAttributed"), 0.0) AS "TinyQueriesPercent",
        COALESCE(DIV0(Queries."CreditsForIngestion", Queries."TotalCreditsAttributed"), 0.0) AS "IngestionPercent",
        COALESCE(DIV0(Queries."CreditsForComplexJoins", Queries."TotalCreditsAttributed"), 0.0) AS "ComplexJoinsPercent",
        COALESCE(DIV0(Queries."CreditsForOther", Queries."TotalCreditsAttributed"), 0.0) AS "OtherPercent",
        IFF(
            COALESCE(Queries."TotalCreditsAttributed", 0.0) = 0.0,
            1.0,
            COALESCE(DIV0(Queries."WastedCredits", Queries."TotalCreditsAttributed"), 0.0)
        ) AS "WastedPercent",
        COALESCE(DIV0(Queries."BlocksForHeavyScans", Queries."TotalBlocks"), 0.0) AS "HeavyScansBlocksPercent",
        COALESCE(DIV0(Queries."BlocksForTinyQueries", Queries."TotalBlocks"), 0.0) AS "TinyQueriesBlocksPercent",
        COALESCE(DIV0(Queries."BlocksForComplexJoins", Queries."TotalBlocks"), 0.0) AS "ComplexJoinsBlocksPercent",
        COALESCE(DIV0(Queries."BlocksForOther", Queries."TotalBlocks"), 0.0) AS "OtherBlocksPercent"
    FROM CreditsPerHourPerWarehouse AS Credits
    LEFT JOIN QueryAnalysisPerWarehousePerHour AS Queries
        ON Credits."WarehouseName" = Queries."WarehouseName"
       AND Credits."AggregateTime" = Queries."AggregateTime"
),
UsageCalculated AS (
    SELECT
        SC.*,
        SC."HeavyScansPercent" * SC."TotalCreditsBilled" * M.HeavyScansMultiplier AS "HeavyScansUsage",
        SC."TinyQueriesPercent" * SC."TotalCreditsBilled" * M.TinyQueriesMultiplier AS "TinyQueriesUsage",
        SC."ComplexJoinsPercent" * SC."TotalCreditsBilled" * M.ComplexJoinsMultiplier AS "ComplexJoinsUsage",
        0 AS "IngestionUsage",
        SC."OtherPercent" * SC."TotalCreditsBilled" * M.OtherMultiplier AS "OtherUsage"
    FROM StructureCalculated AS SC
    CROSS JOIN Multipliers AS M
)
SELECT
    "WarehouseName",
    "AggregateTime",
    "NodesCount",
    "QueryCount",
    "ActiveClusters",
    "QueuedOverloadSeconds",
    "QueuedQueries",
    "TotalCreditsBilled",
    "TotalCreditsAttributed",
    "WastedCredits",
    "TotalBlocks",
    "BlocksForHeavyScans",
    "BlocksForTinyQueries",
    "BlocksForComplexJoins",
    "BlocksForOther",
    "HeavyScansPercent",
    "TinyQueriesPercent",
    "IngestionPercent",
    "ComplexJoinsPercent",
    "WastedPercent",
    "HeavyScansBlocksPercent",
    "TinyQueriesBlocksPercent",
    "ComplexJoinsBlocksPercent",
    "OtherPercent",
    "OtherBlocksPercent",
    "HeavyScansUsage",
    "TinyQueriesUsage",
    "ComplexJoinsUsage",
    "IngestionUsage",
    "OtherUsage",
    ("HeavyScansUsage" + "TinyQueriesUsage" + "ComplexJoinsUsage" + "IngestionUsage" + "OtherUsage") AS "TotalUsage"
FROM UsageCalculated
ORDER BY "AggregateTime" ASC, "WarehouseName" ASC;
