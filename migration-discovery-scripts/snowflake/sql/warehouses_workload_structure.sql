SET LOOKBACK_DAYS = 14;
WITH CreditsPerHourPerWarehouse AS (
    SELECT
        WAREHOUSE_NAME AS "WarehouseName",
        DATE_TRUNC('HOUR', START_TIME) AS "AggregateTime",
        SUM(CREDITS_USED_COMPUTE) AS "TotalCreditsBilled"
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE START_TIME >= DATEADD('day', -$LOOKBACK_DAYS, CURRENT_TIMESTAMP())
      AND WAREHOUSE_NAME IS NOT NULL
    GROUP BY 1, 2
),
MultiObjectQueries AS (
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
        QL.QUERY_TYPE,
        QL.USER_NAME,
        COALESCE(QL.BYTES_SCANNED, 0) AS "BytesScanned",
        COALESCE(QL.ROWS_PRODUCED, 0) AS "RowsProduced",
        GREATEST(
            CEIL(COALESCE(QL.BYTES_SCANNED, 0) / 1048576.0), 10
        ) AS "QueryBlocks",
        MOQ.QUERY_ID IS NOT NULL AS "IsMultiObject",
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
    LEFT JOIN MultiObjectQueries AS MOQ
        ON QL.QUERY_ID = MOQ.QUERY_ID
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
),
QueryAnalysisPerWarehousePerHour AS (
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
        SUM(IFF(USER_NAME = 'SYSTEM', "AttributedCredits", 0.0)) AS "CreditsForSystem",
        SUM(IFF("IsFilteringAbove1Gb", "AttributedCredits", 0.0))
            AS "CreditsForFilteringAbove1Gb",
        SUM(IFF("IsFilteringAbove1Gb", "QueryBlocks", 0)) AS "BlocksForFilteringAbove1Gb",
        SUM(IFF("IsLowCost", "AttributedCredits", 0.0)) AS "CreditsForLowCost",
        SUM(IFF("IsLowCost", "QueryBlocks", 0)) AS "BlocksForLowCost",
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
),
HourlyStructure AS (
    SELECT
        Credits."WarehouseName",
        Credits."AggregateTime",
        COALESCE(Queries."TotalBlocks", 0) AS "TotalBlocks",
        COALESCE(Queries."BlocksForFilteringAbove1Gb", 0) AS "BlocksForFilteringAbove1Gb",
        COALESCE(Queries."BlocksForLowCost", 0) AS "BlocksForLowCost",
        COALESCE(Queries."BlocksForMultiObject", 0) AS "BlocksForMultiObject",
        COALESCE(Queries."BlocksForStandard", 0) AS "BlocksForStandard",
        COALESCE(DIV0(
            Queries."CreditsForFilteringAbove1Gb", Queries."TotalCreditsAttributed"
        ), 0.0) AS "FilteringAbove1GbCreditRatio",
        COALESCE(DIV0(
            Queries."CreditsForLowCost", Queries."TotalCreditsAttributed"
        ), 0.0) AS "LowCostCreditRatio",
        COALESCE(DIV0(
            Queries."CreditsForCopy", Queries."TotalCreditsAttributed"
        ), 0.0) AS "CopyCreditRatio",
        COALESCE(DIV0(
            Queries."CreditsForMultiObject", Queries."TotalCreditsAttributed"
        ), 0.0) AS "MultiObjectCreditRatio",
        COALESCE(DIV0(
            Queries."CreditsForStandard", Queries."TotalCreditsAttributed"
        ), 0.0) AS "StandardCreditRatio",
        IFF(
            COALESCE(Queries."TotalCreditsAttributed", 0.0) = 0.0,
            1.0,
            COALESCE(DIV0(
                Queries."CreditsForSystem", Queries."TotalCreditsAttributed"
            ), 0.0)
        ) AS "SystemCreditRatio",
        COALESCE(DIV0(
            Queries."BlocksForFilteringAbove1Gb", Queries."TotalBlocks"
        ), 0.0) AS "FilteringAbove1GbBlockRatio",
        COALESCE(DIV0(
            Queries."BlocksForLowCost", Queries."TotalBlocks"
        ), 0.0) AS "LowCostBlockRatio",
        COALESCE(DIV0(
            Queries."BlocksForMultiObject", Queries."TotalBlocks"
        ), 0.0) AS "MultiObjectBlockRatio",
        COALESCE(DIV0(
            Queries."BlocksForStandard", Queries."TotalBlocks"
        ), 0.0) AS "StandardBlockRatio"
    FROM CreditsPerHourPerWarehouse AS Credits
    LEFT JOIN QueryAnalysisPerWarehousePerHour AS Queries
        ON Credits."WarehouseName" = Queries."WarehouseName"
       AND Credits."AggregateTime" = Queries."AggregateTime"
)
SELECT
    "WarehouseName",
    AVG("TotalBlocks") AS "AvgTotalBlocks",
    AVG("BlocksForFilteringAbove1Gb") AS "AvgFilteringAbove1GbBlocks",
    AVG("BlocksForLowCost") AS "AvgLowCostBlocks",
    AVG("BlocksForMultiObject") AS "AvgMultiObjectBlocks",
    AVG("BlocksForStandard") AS "AvgStandardBlocks",
    AVG("FilteringAbove1GbCreditRatio") AS "FilteringAbove1GbCreditRatio",
    AVG("LowCostCreditRatio") AS "LowCostCreditRatio",
    AVG("CopyCreditRatio") AS "CopyCreditRatio",
    AVG("MultiObjectCreditRatio") AS "MultiObjectCreditRatio",
    AVG("StandardCreditRatio") AS "StandardCreditRatio",
    AVG("SystemCreditRatio") AS "SystemCreditRatio",
    AVG("FilteringAbove1GbBlockRatio") AS "FilteringAbove1GbBlockRatio",
    AVG("LowCostBlockRatio") AS "LowCostBlockRatio",
    AVG("MultiObjectBlockRatio") AS "MultiObjectBlockRatio",
    AVG("StandardBlockRatio") AS "StandardBlockRatio"
FROM HourlyStructure
GROUP BY "WarehouseName"
ORDER BY "WarehouseName" ASC;
