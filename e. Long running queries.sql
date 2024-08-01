call OBSERVABILITY_SETUP.SETUP_PROCEDURES_V1.CHILD_LONG_RUNNING_QUERY_ALERT('alonso.sevilla@wahlclipper.com','david.nelsen@wahlclipper.com');
CREATE OR REPLACE PROCEDURE OBSERVABILITY_SETUP.SETUP_PROCEDURES_V1.CHILD_LONG_RUNNING_QUERY_ALERT(INFRA_EMAIL VARCHAR, CUSTOMER_EMAIL VARCHAR)
    RETURNS VARIANT
    LANGUAGE javascript
    EXECUTE AS CALLER
    AS
    $$

    var costing_db_name = "KH_SNOWFLAKE_COSTING";
    
    var infra_email = INFRA_EMAIL;
    var customer_email = CUSTOMER_EMAIL;

    var account_query = "SELECT CURRENT_ACCOUNT()";
    var statement = snowflake.createStatement({sqlText: account_query});
    var account = statement.execute()
    account.next();
    account_name = account.getColumnValue(1);

    var query_db = "SELECT CURRENT_ACCOUNT()|| '_OBSERVABILITY'";
    var statement1 = snowflake.createStatement({sqlText: query_db});
    var db = statement1.execute();
    db.next();
    db_name = db.getColumnValue(1);

    alert_query = `
    BEGIN
    CREATE OR REPLACE ALERT `+db_name+`.OBSERVABILITY_CORE.LONG_RUNNING_ALERT
      WAREHOUSE = 'OBSERVABILITY_WH'
      SCHEDULE = '1440 MINUTE'
      IF( EXISTS(
        WITH WAREHOUSE_SIZES AS (
          SELECT 'X-Small' AS WAREHOUSE_SIZE, 1 AS CREDITS_PER_HOUR UNION ALL
          SELECT 'Small' AS WAREHOUSE_SIZE, 2 AS CREDITS_PER_HOUR UNION ALL
          SELECT 'Medium'  AS WAREHOUSE_SIZE, 4 AS CREDITS_PER_HOUR UNION ALL
          SELECT 'Large' AS WAREHOUSE_SIZE, 8 AS CREDITS_PER_HOUR UNION ALL
          SELECT 'X-Large' AS WAREHOUSE_SIZE, 16 AS CREDITS_PER_HOUR UNION ALL
          SELECT '2X-Large' AS WAREHOUSE_SIZE, 32 AS CREDITS_PER_HOUR UNION ALL
          SELECT '3X-Large' AS WAREHOUSE_SIZE, 64 AS CREDITS_PER_HOUR UNION ALL
          SELECT '4X-Large' AS WAREHOUSE_SIZE, 128 AS CREDITS_PER_HOUR
        )
        ,query as (
        SELECT CURRENT_ACCOUNT() AS ACCOUNT,
          'LONG_RUNNING_QUERY' AS METRICNAME,
          QH.WAREHOUSE_NAME AS DESCRIPTION,
          START_TIME,
          QUERY_ID QUERYID,
          INBOUND_DATA_TRANSFER_BYTES AS DATATRANSFER,
          EXECUTION_TIME/(1000*60*60)*WH.CREDITS_PER_HOUR AS COMPUTECOST,
          IFF(((QH.WAREHOUSE_SIZE = 'X-Small' 
            OR QH.WAREHOUSE_SIZE = 'Small' 
            OR QH.WAREHOUSE_SIZE = 'Medium'
            OR QH.WAREHOUSE_SIZE = 'Large'
            OR QH.WAREHOUSE_SIZE = 'X-Large') and EXECUTION_TIME > 25200000),1,0) XL_FLAG,
          IFF(((QH.WAREHOUSE_SIZE = '2X-Large'
            OR QH.WAREHOUSE_SIZE = '3X-Large') and EXECUTION_TIME > 18000000), 1, 0) THREEXL_FLAG,
          IFF((QH.WAREHOUSE_SIZE = '4X-Large' and EXECUTION_TIME > 9000000), 1,0) FOURXL_FLAG
          from TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY()) QH
          INNER JOIN WAREHOUSE_SIZES WH
            ON QH.WAREHOUSE_SIZE=WH.WAREHOUSE_SIZE
          WHERE EXECUTION_STATUS='RUNNING' 
          )
          SELECT ACCOUNT,
            METRICNAME,
            DESCRIPTION,
            START_TIME,
            QUERYID
          FROM QUERY QH
          WHERE XL_FLAG = 1 OR THREEXL_FLAG = 1 OR FOURXL_FLAG = 1
              AND EXISTS (
                SELECT QUERYID
                FROM `+db_name+`.OBSERVABILITY_CORE.OBSERVABILITY_METRICS OB
                WHERE QH.QUERYID = OB.QUERYID
                AND QH.METRICNAME = OB.METRICNAME)))
          THEN CALL SYSTEM$SEND_EMAIL('ALERT_INFRA', '` + infra_email + `, ` + customer_email + `', 'NEW METRICS ALERT ON ACCOUNT `+account_name+`', 'THERES A QUERY RUNNING FOR 25% ABOVE THE THRESHOLD TIME');
          INSERT INTO OBSERVABILITY_MAINTENANCE.LOGGING_SCHEMA.LOGGING_TABLE
            VALUES ('CHILD_LONG_RUNNING_QUERY_ALERT','SUCCESS', CURRENT_TIMESTAMP(), '`+account_name+`');
          RETURN 'SUCCESS';
          EXCEPTION
          WHEN STATEMENT_ERROR THEN
              LET ERROR_MESSAGE := CURRENT_TIMESTAMP()||SQLCODE || ': '||SQLERRM;
              INSERT INTO OBSERVABILITY_MAINTENANCE.LOGGING_SCHEMA.LOGGING_TABLE
              VALUES('CHILD_LONG_RUNNING_QUERY_ALERT','FAILED', :ERROR_MESSAGE,'`+account_name+`' );
              USE ROLE ACCOUNTADMIN;
              USE WAREHOUSE COMPUTE_WH;
      RETURN OBJECT_CONSTRUCT('ERROR TYPE','STATEMENT_ERROR',
                              'SQLCODE', SQLCODE,
                              'SQLERRM' , SQLERRM,
                              'SQLSTATE', SQLSTATE);
      END;
    `;

    var statement2 = snowflake.createStatement({sqlText: alert_query});
    var result1 = statement2.execute();
    result1.next();
    res1 = result1.getColumnValue(1);

    resume_query = `ALTER ALERT `+db_name+`.OBSERVABILITY_CORE.LONG_RUNNING_ALERT RESUME`;
    var statement3 = snowflake.createStatement({sqlText: resume_query});
    var result2 = statement3.execute();
    result2.next();
    res2 = result2.getColumnValue(1);
    return res1.concat(' ', res2) 
    $$;