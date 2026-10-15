SHOW WAREHOUSES;

SELECT
    "name" AS "Name",
    "state" AS "State",
    "type" AS "Type",
    "size" AS "Size",
    "min_cluster_count" AS "MinClusterCount",
    "max_cluster_count" AS "MaxClusterCount",
    "auto_suspend" AS "AutoSuspend",
    "auto_resume" AS "AutoResume",
    "created_on" AS "CreatedOn",
    "resumed_on" AS "ResumedOn",
    "updated_on" AS "UpdatedOn",
    "comment" AS "Comment",
    "enable_query_acceleration" AS "EnableQueryAcceleration",
    "query_acceleration_max_scale_factor" AS "QueryAccelerationMaxScaleFactor",
    "resource_monitor" AS "ResourceMonitor",
    "scaling_policy" AS "ScalingPolicy"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
