CALL OBSERVABILITY_SETUP.setup_procedures_v1.CHILD_KH_SNOWFLAKE_COSTING_CREDITS_BY_QUERY_PROCEDURE();
CREATE OR REPLACE PROCEDURE OBSERVABILITY_SETUP.setup_procedures_v1.CHILD_KH_SNOWFLAKE_COSTING_CREDITS_BY_QUERY_PROCEDURE()
      RETURNS VARCHAR(16777216)
      LANGUAGE JAVASCRIPT
      EXECUTE AS CALLER
      AS 
      $$
          var costing_db_name = "KH_SNOWFLAKE_COSTING";
          
          var account_query = "SELECT CURRENT_ACCOUNT()";
          var statement = snowflake.createStatement({sqlText: account_query});
          var account = statement.execute()
          account.next();
          var account_name = account.getColumnValue(1);
          
          var query_db = `SELECT CURRENT_ACCOUNT()||'_${costing_db_name}'`
          var statement1 = snowflake.createStatement({sqlText: query_db});
          var db = statement1.execute();
          db.next();
          var db_name = db.getColumnValue(1);

          var query1 = `
              BEGIN 
                  IF (EXISTS(SELECT * FROM `+db_name+`.SNOWFLAKE_COST_STD.QUERY_COST_PARAMETERS)) THEN 
                      RETURN 'No update needed to QUERY_COST_PARAMETERS';
                  ELSE 
                      INSERT INTO `+db_name+`.SNOWFLAKE_COST_STD.QUERY_COST_PARAMETERS (PARAMETER_NAME, PARAM_VALUE_TIMESTAMP)
                      VALUES ('query_start_time','2022-10-01'::timestamp_ltz);
                      RETURN 'Successful update to QUERY_COST_PARAMETERS';
                  END IF;
              END;`
          var statement2 = snowflake.createStatement({sqlText: query1});
          var result1 = statement2.execute();
          result1.next();

          var query2 = ` 
              DECLARE 
              START_TIME TIMESTAMP_LTZ; --DEFAULT DATE_TRUNC('HOUR', DATEADD('DAY', -25, CURRENT_TIMESTAMP()));
              END_TIME TIMESTAMP_LTZ; --DEFAULT DATE_TRUNC('HOUR', DATEADD('HOUR', -4, CURRENT_TIMESTAMP()));
              TIME_INTERVAL VARCHAR DEFAULT 'HOUR';
              ROWCOUNT NUMBER;
              
              BEGIN
              SELECT DATEADD('DAY', -7, PARAM_VALUE_TIMESTAMP) AS LAST_QUERY_START_TIMESTAMP
              , DATE_TRUNC(:TIME_INTERVAL,DATEADD('HOUR',-4,CURRENT_TIMESTAMP()))::TIMESTAMP_LTZ AS END_TIME
              INTO :START_TIME, :END_TIME
              FROM `+db_name+`.SNOWFLAKE_COST_STD.QUERY_COST_PARAMETERS
              WHERE PARAMETER_NAME = 'query_start_time'
              ORDER BY PARAM_VALUE_TIMESTAMP DESC
              LIMIT 1;;
              
              DELETE FROM `+db_name+`.SNOWFLAKE_COST_STD.SF_CREDITS_BY_QUERY_NEW WHERE START_SLICE >= :START_TIME;
              
              ROWCOUNT := (SELECT DATEDIFF(:TIME_INTERVAL, :START_TIME, :END_TIME));
              
              INSERT INTO `+db_name+`.SNOWFLAKE_COST_STD.SF_CREDITS_BY_QUERY_NEW (
                    ACCOUNT_NAME,
                    QUERY_ID,
                      START_SLICE,
                      ADJUSTED_START_TIME,
                      START_TIME,
                      END_TIME,
                      QUERY_TYPE,
                      QUERY_TEXT,
                      QUERY_TAG,
                      DATABASE_ID,
                      DATABASE_NAME,
                      SCHEMA_NAME,
                      SESSION_ID,
                      USER_NAME,
                      ROLE_NAME,
                      WAREHOUSE_NAME,
                      WAREHOUSE_ID,
                      WAREHOUSE_SIZE,
                      WAREHOUSE_TYPE,
                      CLUSTER_NUMBER,
                      TOTAL_ELAPSED_TIME,
                      TOTAL_QUEUE_TIME,
                      TRANSACTION_BLOCKED_TIME,
                      COMPILATION_TIME,
                      DERIVED_ELAPSED_TIME_MS,
                      ELAPSED_TIME_RATIO,
                      ALLOCATED_CREDITS_USED,
                      ALLOCATED_CREDITS_USED_COMPUTE,
                      ALLOCATED_CREDITS_USED_CLOUD_SERVICES
                  )
              WITH TIME_SLICE_BY_INTERVAL AS (
                  SELECT
                      SEQ4() AS SEQ_NUM,
                      ROW_NUMBER() OVER (
                          ORDER BY
                              SEQ_NUM
                      ) AS INDEX,
                      :ROWCOUNT + 1 - INDEX ROW_SEQ_NUM,
                      DATEADD(
                          :TIME_INTERVAL,
                          -1 * INDEX,
                          :END_TIME)::TIMESTAMP_LTZ AS START_SLICE,
                          DATEADD(:TIME_INTERVAL, 1, START_SLICE) AS END_SLICE
                          FROM
                              TABLE(GENERATOR(ROWCOUNT =>(:ROWCOUNT)))
                      ),
                      SF_WAREHOUSE_METERING_HISTORY AS(
                          SELECT
                              START_TIME,
                              WAREHOUSE_ID,
                              WAREHOUSE_NAME,
                              CREDITS_USED,
                              CREDITS_USED_COMPUTE,
                              CREDITS_USED_CLOUD_SERVICES
                          FROM
                              SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                          WHERE
                              START_TIME >= :START_TIME
                              AND START_TIME < :END_TIME
                              AND WAREHOUSE_ID != 0 --DON'T PULL CLOUD SERVICES RECORDS, THESE CAN'T BE TIED TO A QUERY
                      ),
                      SF_QUERY_HISTORY AS (
                          SELECT
                              DATABASE_ID,
                              DATABASE_NAME,
                              SCHEMA_NAME,
                              QUERY_TYPE,
                              USER_NAME,
                              ROLE_NAME,
                              WAREHOUSE_ID,
                              WAREHOUSE_NAME,
                              WAREHOUSE_SIZE,
                              CLUSTER_NUMBER,
                              QUERY_TAG,
                              START_TIME,
                              END_TIME,
                              SESSION_ID,
                              TOTAL_ELAPSED_TIME,
                              CREDITS_USED_CLOUD_SERVICES,
                              QUEUED_PROVISIONING_TIME,
                              QUEUED_REPAIR_TIME,
                              QUEUED_OVERLOAD_TIME,
                              QUERY_ID,
                              QUERY_TEXT,
                              WAREHOUSE_TYPE,
                              COALESCE(Q.QUEUED_PROVISIONING_TIME, 0) + COALESCE(Q.QUEUED_REPAIR_TIME, 0) + COALESCE(Q.QUEUED_OVERLOAD_TIME, 0) AS TOTAL_QUEUE_TIME,
                              COALESCE(TRANSACTION_BLOCKED_TIME,0) AS TRANSACTION_BLOCKED_TIME, 
                              COMPILATION_TIME,
                              DATEADD(
                                  'MILLISECOND',(TOTAL_QUEUE_TIME+COALESCE(Q.TRANSACTION_BLOCKED_TIME,0)+COALESCE(COMPILATION_TIME,0)),
                                  Q.START_TIME )::TIMESTAMP_LTZ AS ADJUSTED_START_TIME,
                                  DATE_TRUNC(:TIME_INTERVAL, ADJUSTED_START_TIME) AS ADJUSTED_START_INTERVAL
                                  FROM
                                      SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY Q
                                  WHERE Q.START_TIME >= DATEADD('DAY',-7, :START_TIME)
                                      AND Q.START_TIME < :END_TIME 
                                      AND WAREHOUSE_SIZE IS NOT NULL
                                      AND QUERY_TYPE NOT IN (
                                          'GET_FILES',
                                          'LIST_FILES',
                                          'PUT_FILES',
                                          'REMOVE_FILES',
                                          'ALTER SESSION',
                                          'DESCRIBE',
                                          'SHOW',
                                          'USE'
                                      ) 
                              ), 
                              QUERIES_BY_TIME_SLICE AS (
                                  SELECT
                                      QH.QUERY_ID,
                                      QH.QUERY_TYPE,
                                      QH.QUERY_TEXT,
                                      QH.QUERY_TAG,
                                      QH.DATABASE_ID,
                                      QH.DATABASE_NAME,
                                      QH.SCHEMA_NAME,
                                      QH.SESSION_ID,
                                      QH.USER_NAME,
                                      QH.ROLE_NAME,
                                      QH.WAREHOUSE_NAME,
                                      QH.WAREHOUSE_ID,
                                      QH.WAREHOUSE_SIZE,
                                      QH.WAREHOUSE_TYPE,
                                      QH.CLUSTER_NUMBER,
                                      QH.START_TIME,
                                      QH.END_TIME,
                                      QH.TOTAL_ELAPSED_TIME,
                                      QH.TOTAL_QUEUE_TIME,
                                      QH.TRANSACTION_BLOCKED_TIME,
                                      QH.COMPILATION_TIME,
                                      QH.ADJUSTED_START_TIME,
                                      DD.START_SLICE,
                                      DATEDIFF(
                                          'MILLISECOND',
                                          GREATEST(DD.START_SLICE, QH.ADJUSTED_START_TIME),
                                          LEAST(DD.END_SLICE, QH.END_TIME)
                                      ) AS DERIVED_ELAPSED_TIME_MS,
                                      RATIO_TO_REPORT(DERIVED_ELAPSED_TIME_MS::NUMBER(38, 8
                              )
                      ) OVER (
                          PARTITION BY QH.WAREHOUSE_ID,
                          DD.START_SLICE
                      ) AS ELAPSED_TIME_RATIO
                  FROM
                      SF_QUERY_HISTORY QH
                      INNER JOIN TIME_SLICE_BY_INTERVAL DD ON DD.END_SLICE 
                                  BETWEEN QH.ADJUSTED_START_TIME
                      AND DATEADD(:TIME_INTERVAL, 1, QH.END_TIME)
                      /* ON QH.ADJUSTED_START_TIME < DD.END_SLICE
                              AND DD.START_SLICE < QH.END_TIME*/
                  WHERE
                      DD.START_SLICE >= :START_TIME
                      AND DD.START_SLICE < :END_TIME
              )
              SELECT
                  CURRENT_ACCOUNT(),
                  Q.QUERY_ID,
                  COALESCE(Q.START_SLICE,WMH.START_TIME) AS START_SLICE, 
                  Q.ADJUSTED_START_TIME,
                  Q.START_TIME,
                  Q.END_TIME,
                  Q.QUERY_TYPE,
                  Q.QUERY_TEXT,
                  Q.QUERY_TAG,
                  Q.DATABASE_ID,
                  Q.DATABASE_NAME,
                  Q.SCHEMA_NAME,
                  Q.SESSION_ID,
                  Q.USER_NAME,
                  Q.ROLE_NAME,
                  COALESCE(Q.WAREHOUSE_NAME, WMH.WAREHOUSE_NAME) WAREHOUSE_NAME,
                  COALESCE(Q.WAREHOUSE_ID, WMH.WAREHOUSE_ID) WAREHOUSE_ID,
                  Q.WAREHOUSE_SIZE,
                  Q.WAREHOUSE_TYPE,
                  Q.CLUSTER_NUMBER,
                  Q.TOTAL_ELAPSED_TIME,
                  Q.TOTAL_QUEUE_TIME,
                  Q.TRANSACTION_BLOCKED_TIME,
                  Q.COMPILATION_TIME,
                  Q.DERIVED_ELAPSED_TIME_MS,
                  Q.ELAPSED_TIME_RATIO,
                  NVL(ELAPSED_TIME_RATIO, 1) * WMH.CREDITS_USED AS ALLOCATED_CREDITS_USED,
                  NVL(ELAPSED_TIME_RATIO, 1) * WMH.CREDITS_USED_COMPUTE AS ALLOCATED_CREDITS_USED_COMPUTE,
                  NVL(ELAPSED_TIME_RATIO, 1) * WMH.CREDITS_USED_CLOUD_SERVICES AS ALLOCATED_CREDITS_USED_CLOUD_SERVICES
              FROM
                  SF_WAREHOUSE_METERING_HISTORY WMH
                  LEFT OUTER JOIN QUERIES_BY_TIME_SLICE Q ON WMH.WAREHOUSE_NAME = Q.WAREHOUSE_NAME
                  AND WMH.WAREHOUSE_ID = Q.WAREHOUSE_ID
                  AND Q.START_SLICE BETWEEN WMH.START_TIME AND DATEADD('MILLISECOND',-1, DATEADD('HOUR',1,WMH.START_TIME))
                  --AND WMH.START_TIME = Q.START_HOUR
              ORDER BY
                  WMH.START_TIME;
              
              UPDATE `+db_name+`.SNOWFLAKE_COST_STD.QUERY_COST_PARAMETERS 
              SET PARAM_VALUE_TIMESTAMP =:END_TIME::TIMESTAMP_LTZ
              WHERE PARAMETER_NAME = 'query_start_time';
              
              INSERT INTO `+db_name+`.SNOWFLAKE_COST_STD.CREDITS_ALLOCATION_VALIDATION_RESULTS(
                  CREATED_AT_TS, START_TIME, END_TIME, SOURCE_DATA_SOURCE, TARGET_DATA_SOURCE, 
                  SOURCE_CREDITS_USED, TARGET_CREDITS_USED, METERING_HOUR_MIN, METERING_HOUR_MAX, START_SLICE_MIN, START_SLICE_MAX,
                  CREDITS_USED_DIFF, METERING_HOUR_MIN_DIFF_SECONDS, METERING_HOUR_MAX_DIFF_SECONDS) 
              WITH SRC AS (
                  SELECT 'ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY' AS DATA_SOURCE
                      , SUM(CREDITS_USED) CREDITS_USED 
                      , MIN(START_TIME) AS METERING_HOUR_MIN
                      , MAX(START_TIME) AS METERING_HOUR_MAX 
                  FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                  WHERE START_TIME >= :START_TIME 
                  AND START_TIME < :END_TIME 
                  AND WAREHOUSE_ID != 0
                  GROUP BY 1 
              ), 
              TGT AS (
                  SELECT 'SF_CREDITS_BY_QUERY' AS DATA_SOURCE
                      , SUM(ALLOCATED_CREDITS_USED) ALLOCATED_CREDITS_USED_SUM 
                      , MIN(START_SLICE) AS START_SLICE_MIN 
                      , MAX(START_SLICE) AS START_SLICE_MAX 
                  FROM  `+db_name+`.SNOWFLAKE_COST_STD.SF_CREDITS_BY_QUERY_NEW
                  WHERE START_SLICE >= :START_TIME 
                  AND START_SLICE < :END_TIME 
                  AND WAREHOUSE_ID != 0 
                  GROUP BY 1 
              )
              SELECT 
                  CURRENT_TIMESTAMP AS CREATED_AT_TS
                  , :START_TIME AS START_TIME 
                  , :END_TIME AS END_TIME 
                  , S.DATA_SOURCE AS SOURCE_DATA_SOURCE
                  , T.DATA_SOURCE AS TARGET_DATA_SOURCE
                  , ROUND(S.CREDITS_USED,4) AS SOURCE_CREDITS_USED 
                  , ROUND(T.ALLOCATED_CREDITS_USED_SUM,4) AS TARGET_CREDITS_USED 
                  , S.METERING_HOUR_MIN
                  , S.METERING_HOUR_MAX
                  , T.START_SLICE_MIN
                  , T.START_SLICE_MAX
                  , COALESCE(SOURCE_CREDITS_USED,0) - COALESCE(TARGET_CREDITS_USED,0) AS CREDITS_USED_DIFF
                  , DATEDIFF('SECOND', S.METERING_HOUR_MIN, T.START_SLICE_MIN) AS METERING_HOUR_MIN_DIFF_SECONDS
                  , DATEDIFF('SECOND', S.METERING_HOUR_MAX, T.START_SLICE_MAX) AS METERING_HOUR_MAX_DIFF_SECONDS
              FROM SRC S
              CROSS JOIN TGT T
              ;
              
              RETURN ('ROWCOUNT:'||ROWCOUNT|| ' START_TIME:'||START_TIME||' END_TIME:'||END_TIME);
              
              EXCEPTION
                WHEN STATEMENT_ERROR THEN
                  RETURN OBJECT_CONSTRUCT('ERROR TYPE','STATEMENT_ERROR',
                                          'SQLCODE', SQLCODE,
                                          'SQLERRM' , SQLERRM,
                                          'SQLSTATE', SQLSTATE);
                WHEN OTHER THEN
                  RETURN OBJECT_CONSTRUCT('ERROR TYPE', 'OTHER ERROR',
                                          'SQLCODE', SQLCODE,
                                          'SQLERRM', SQLERRM,
                                          'SQLSTATE', SQLSTATE);
              
              END;`
          var statement3 = snowflake.createStatement({sqlText: query2});
          var result2 = statement3.execute();
          result2.next();
          return result2.getColumnValue(1);
    $$;