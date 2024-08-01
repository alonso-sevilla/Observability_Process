CALL observability_setup.setup_procedures_v1.CHILD_DB_SCHEMA_TABLE_STANDUP();
CREATE OR REPLACE PROCEDURE observability_setup.setup_procedures_v1.CHILD_DB_SCHEMA_TABLE_STANDUP()
      RETURNS VARCHAR(16777216)
      LANGUAGE JAVASCRIPT
      EXECUTE AS CALLER
    AS 
    $$
      var costing_db_name = "KH_SNOWFLAKE_COSTING";

      var warehouse_query = "USE WAREHOUSE OBSERVABILITY_WH";
      var wh_statement = snowflake.createStatement({sqlText: warehouse_query});
      var warehouse_execute = wh_statement.execute();
      
      
      var account_query = "SELECT CURRENT_ACCOUNT()";
      var statement0 = snowflake.createStatement({sqlText: account_query});
      var account = statement0.execute();
      account.next();
      account_name = account.getColumnValue(1);
      
      var query_db = "SELECT CURRENT_ACCOUNT()|| '_OBSERVABILITY'";
      var statement1 = snowflake.createStatement({sqlText: query_db});
      var db = statement1.execute();
      db.next();
      db_name = db.getColumnValue(1);
      
      var query_schema = "SELECT CURRENT_ACCOUNT()|| '_OBSERVABILITY.OBSERVABILITY_CORE'";
      var statement2 = snowflake.createStatement({sqlText: query_schema});
      var schema = statement2.execute();
      schema.next();
      schema_name = schema.getColumnValue(1);

      var query_table = "SELECT CURRENT_ACCOUNT()|| '_OBSERVABILITY.OBSERVABILITY_CORE.OBSERVABILITY_METRICS'";
      var statement3 = snowflake.createStatement({sqlText: query_table});
      var table = statement3.execute();
      table.next();
      table_name = table.getColumnValue(1);
      
              var warehouse_execute = wh_statement.execute();
      
      var sql_query = `
        BEGIN
        CREATE DATABASE IF NOT EXISTS ` + db_name + `;

        CREATE SCHEMA IF NOT EXISTS ` + schema_name + `;

        CREATE OR REPLACE TABLE ` + table_name + ` (
        ACCOUNT VARCHAR(16777216),
    	METRICNAME VARCHAR(16777216),
    	TIMESTAMP TIMESTAMP_NTZ(9),
    	DESCRIPTION VARCHAR(16777216),
    	QUERYID VARCHAR(16777216),
    	DATATRANSFER FLOAT,
    	TOTALCOST FLOAT,
    	COMPUTECOST FLOAT,
    	STORAGECOST FLOAT,
    	LONGRUNNINGQUERY VARCHAR(16777216),
    	TOPWAREHOUSE ARRAY,
    	TOPUSER ARRAY,
    	TOPROLE ARRAY,
    	QUERYTIMEOUT VARCHAR(16777216),
    	TASKCREDITS VARCHAR(16777216),
    	WAREHOUSEPROVISION VARCHAR(16777216),
    	QUERYQUEUEING VARCHAR(16777216),
    	WAREHOUSEREPAIR VARCHAR(16777216),
    	SERVICEACCOUNT VARCHAR(16777216),
    	ADMINLOGIN VARCHAR(16777216),
    	UNUSEDROLE VARCHAR(16777216),
    	NOTALLOWEDUSER VARCHAR(16777216)
        );

        ALTER TABLE ` + table_name + ` SET CHANGE_TRACKING = TRUE;

        CREATE TABLE IF NOT EXISTS ` + schema_name +`.ALLOWED_USERADMINS (
          ACCOUNT VARCHAR,
          USERNAME VARCHAR,
          ROLE VARCHAR
        );
        
        INSERT INTO OBSERVABILITY_MAINTENANCE.LOGGING_SCHEMA.LOGGING_TABLE
        VALUES ('CHILD_DB_SCHEMA_TABLE_STANDUP','SUCCESS', CURRENT_TIMESTAMP(), '`+account_name+`');
        RETURN 'SUCCESS';
        EXCEPTION
          WHEN statement_error THEN
            LET ERROR_MESSAGE := CURRENT_TIMESTAMP()||SQLCODE || ': '||SQLERRM;
            INSERT INTO observability_maintenance.logging_schema.logging_table
            VALUES('CHILD_DB_SCHEMA_TABLE_STANDUP','FAILED', :ERROR_MESSAGE ,'`+account_name+`');
            USE ROLE ACCOUNTADMIN;
            USE WAREHOUSE COMPUTE_WH;
           
            RETURN OBJECT_CONSTRUCT('ERROR TYPE','STATEMENT_ERROR',
                                        'SQLCODE', sqlcode,
                                        'SQLERRM' , sqlerrm,
                                        'SQLSTATE', sqlstate);
        END`;
      var statement4 = snowflake.createStatement({sqlText: sql_query});
      var result = statement4.execute();
      var kh_query = `
        BEGIN
        CREATE DATABASE IF NOT EXISTS ` + account_name + `_${costing_db_name};

        CREATE SCHEMA IF NOT EXISTS ` + account_name + `_${costing_db_name}.SNOWFLAKE_COST_STD;
        
        create TABLE if not exists ` + account_name + `_${costing_db_name}.SNOWFLAKE_COST_STD.DAILY_STAGE_STORAGE (
          ACCOUNT_NAME VARCHAR(16777216),
          USAGE_DATE DATE,
          AVERAGE_STAGE_BYTES NUMBER(38,6)
        );
        
        CREATE  TABLE IF NOT EXISTS ` + account_name + `_${costing_db_name}.SNOWFLAKE_COST_STD.DB_STORAGE_CALC (
          USAGE_DATE DATE,
          DATABASE_NAME VARCHAR(16777216),
          DATABASE_STORAGE_PETABYTES FLOAT,
          FAILSAFE_STORAGE_PETABYTES FLOAT,
          HYBRID_STORAGE_PETABYTES FLOAT,
          ACCOUNT_NAME VARCHAR(16777216)
        );

        CREATE TRANSIENT TABLE IF NOT EXISTS ` + account_name + `_${costing_db_name}.SNOWFLAKE_COST_STD.SF_CREDITS_BY_QUERY_NEW (
          ACCOUNT_NAME VARCHAR(16777216),
          QUERY_ID VARCHAR(16777216),
          START_SLICE TIMESTAMP_LTZ(0),
          ADJUSTED_START_TIME TIMESTAMP_LTZ(6),
          START_TIME TIMESTAMP_LTZ(6),
          END_TIME TIMESTAMP_LTZ(6),
          QUERY_TYPE VARCHAR(16777216),
          QUERY_TEXT VARCHAR(16777216),
          QUERY_TAG VARCHAR(16777216),
          DATABASE_ID NUMBER(38,0),
          DATABASE_NAME VARCHAR(16777216),
          SCHEMA_NAME VARCHAR(16777216),
          SESSION_ID NUMBER(38,0),
          USER_NAME VARCHAR(16777216),
          ROLE_NAME VARCHAR(16777216),
          WAREHOUSE_NAME VARCHAR(16777216),
          WAREHOUSE_ID NUMBER(38,0),
          WAREHOUSE_SIZE VARCHAR(16777216),
          WAREHOUSE_TYPE VARCHAR(16777216),
          CLUSTER_NUMBER NUMBER(38,0),
          TOTAL_ELAPSED_TIME NUMBER(38,0),
          TOTAL_QUEUE_TIME NUMBER(38,0),
          TRANSACTION_BLOCKED_TIME NUMBER(38,0),
          COMPILATION_TIME NUMBER(38,0),
          DERIVED_ELAPSED_TIME_MS NUMBER(38,0),
          ELAPSED_TIME_RATIO NUMBER(38,12),
          ALLOCATED_CREDITS_USED NUMBER(38,12),
          ALLOCATED_CREDITS_USED_COMPUTE NUMBER(38,12),
          ALLOCATED_CREDITS_USED_CLOUD_SERVICES NUMBER(38,12)
        );

        CREATE TABLE IF NOT EXISTS ` + account_name + `_${costing_db_name}.SNOWFLAKE_COST_STD.CREDITS_ALLOCATION_VALIDATION_RESULTS (
          CREATED_AT_TS TIMESTAMP_LTZ(9),
          START_TIME TIMESTAMP_LTZ(9),
          END_TIME TIMESTAMP_LTZ(9),
          SOURCE_DATA_SOURCE VARCHAR(40),
          TARGET_DATA_SOURCE VARCHAR(19),
          SOURCE_CREDITS_USED NUMBER(38,4),
          TARGET_CREDITS_USED NUMBER(38,4),
          METERING_HOUR_MIN TIMESTAMP_LTZ(0),
          METERING_HOUR_MAX TIMESTAMP_LTZ(0),
          START_SLICE_MIN TIMESTAMP_LTZ(0),
          START_SLICE_MAX TIMESTAMP_LTZ(0),
          CREDITS_USED_DIFF NUMBER(38,4),
          METERING_HOUR_MIN_DIFF_SECONDS NUMBER(18,0),
          METERING_HOUR_MAX_DIFF_SECONDS NUMBER(18,0)
    );

        CREATE TABLE IF NOT EXISTS ` + account_name + `_${costing_db_name}.SNOWFLAKE_COST_STD.QUERY_COST_PARAMETERS(
            PARAMETER_NAME VARCHAR(2000), 
            PARAM_VALUE_TIMESTAMP TIMESTAMP_LTZ, 
            PARAM_VALUE_STRING VARCHAR);
        
        create TABLE IF NOT EXISTS ` + account_name + `_${costing_db_name}.SNOWFLAKE_COST_STD.AUTOMATIC_CLUSTERING_WAREHOUSE_DTLS (
          ACCOUNT_NAME VARCHAR(16777216),
          START_TIME TIMESTAMP_LTZ(6),
          END_TIME TIMESTAMP_LTZ(9),
          CREDITS_USED NUMBER(38,9),
          NUM_BYTES_RECLUSTERED NUMBER(38,0),
          NUM_ROWS_RECLUSTERED NUMBER(38,0),
          TABLE_ID NUMBER(38,0),
          TABLE_NAME VARCHAR(16777216),
          SCHEMA_ID NUMBER(38,0),
          SCHEMA_NAME VARCHAR(16777216),
          DATABASE_ID NUMBER(38,0),
          DATABASE_NAME VARCHAR(16777216)
        );

        INSERT INTO observability_maintenance.logging_schema.logging_table
        VALUES ('CHILD_DB_SCHEMA_TABLE_STANDUP','SUCCESS', CURRENT_TIMESTAMP(), '`+account_name+`');
        RETURN 'SUCCESS';
        EXCEPTION
          WHEN statement_error THEN
            LET ERROR_MESSAGE := CURRENT_TIMESTAMP()||SQLCODE || ': '||SQLERRM;
            INSERT INTO OBSERVABILITY_MAINTENANCE.LOGGING_SCHEMA.LOGGING_TABLE
            VALUES('CHILD_DB_SCHEMA_TABLE_STANDUP','FAILED', :ERROR_MESSAGE,'`+account_name+`' );
            USE ROLE ACCOUNTADMIN;
            USE WAREHOUSE COMPUTE_WH;
            
            RETURN OBJECT_CONSTRUCT('ERROR TYPE','STATEMENT_ERROR',
                                            'SQLCODE', sqlcode,
                                            'SQLERRM' , sqlerrm,
                                            'SQLSTATE', sqlstate);
        END;`
      var statement5 = snowflake.createStatement({sqlText: kh_query});
      var result2 = statement5.execute();

      var role_creation_query = `
        BEGIN
        -- OBSERVABILITY DB ROLE CREATIONS
        USE ROLE SECURITYADMIN;
        CREATE OR REPLACE ROLE ` + db_name + `_ADMIN;
        CREATE OR REPLACE ROLE ` + db_name + `_DEV;
        CREATE OR REPLACE ROLE ` + db_name + `_READER;
        GRANT ROLE ` + db_name + `_ADMIN TO ROLE SYSADMIN;
        GRANT ROLE ` + db_name + `_DEV TO ROLE ` + db_name + `_ADMIN;
        GRANT ROLE ` + db_name + `_READER TO ROLE ` + db_name + `_ADMIN;
        GRANT ROLE ` + db_name + `_ADMIN TO ROLE OBSERVABILITY_ADMIN;
        GRANT ROLE ` + db_name + `_DEV TO ROLE OBSERVABILITY_ADMIN;
        GRANT ROLE ` + db_name + `_READER TO ROLE OBSERVABILITY_ADMIN;
        -- DEV PRIVILEGES
        GRANT USAGE,MONITOR,CREATE SCHEMA ON DATABASE ` + db_name + ` TO ROLE ` + db_name + `_DEV;
        GRANT ALL ON SCHEMA ` + db_name + `.PUBLIC TO ROLE ` + db_name + `_DEV;
        GRANT ALL ON ALL TABLES IN SCHEMA ` + db_name + `.PUBLIC TO ROLE ` + db_name + `_DEV;
        GRANT ALL ON FUTURE TABLES IN SCHEMA ` + db_name + `.PUBLIC TO ROLE ` + db_name + `_DEV;
        GRANT ALL ON ALL VIEWS IN SCHEMA ` + db_name + `.PUBLIC TO ROLE ` + db_name + `_DEV;
        GRANT ALL ON FUTURE VIEWS IN SCHEMA ` + db_name + `.PUBLIC TO ROLE ` + db_name + `_DEV;
        GRANT USAGE,MONITOR,CREATE SCHEMA ON DATABASE ` + db_name + ` TO ROLE ` + db_name + `_DEV;
        GRANT ALL ON SCHEMA ` + db_name + `.OBSERVABILITY_CORE TO ROLE ` + db_name + `_DEV;
        GRANT ALL ON ALL TABLES IN SCHEMA ` + db_name + `.OBSERVABILITY_CORE TO ROLE ` + db_name + `_DEV;
        GRANT ALL ON FUTURE TABLES IN SCHEMA ` + db_name + `.OBSERVABILITY_CORE TO ROLE ` + db_name + `_DEV;
        GRANT ALL ON ALL VIEWS IN SCHEMA ` + db_name + `.OBSERVABILITY_CORE TO ROLE ` + db_name + `_DEV;
        GRANT ALL ON FUTURE VIEWS IN SCHEMA ` + db_name + `.OBSERVABILITY_CORE TO ROLE ` + db_name + `_DEV;
        -- READER PRIVILEGES
        GRANT USAGE,MONITOR ON DATABASE ` + db_name + ` TO ROLE ` + db_name + `_READER;
        GRANT USAGE ON SCHEMA ` + db_name + `.PUBLIC TO ROLE ` + db_name + `_READER;
        GRANT SELECT ON ALL TABLES IN SCHEMA ` + db_name + `.PUBLIC TO ROLE ` + db_name + `_READER;
        GRANT SELECT ON FUTURE TABLES IN SCHEMA ` + db_name + `.PUBLIC TO ROLE ` + db_name + `_READER;
        GRANT SELECT ON ALL VIEWS IN SCHEMA ` + db_name + `.PUBLIC TO ROLE ` + db_name + `_READER;
        GRANT SELECT ON FUTURE VIEWS IN SCHEMA ` + db_name + `.PUBLIC TO ROLE ` + db_name + `_READER;
        GRANT USAGE,MONITOR ON DATABASE ` + db_name + ` TO ROLE ` + db_name + `_READER;
        GRANT USAGE ON SCHEMA ` + db_name + `.OBSERVABILITY_CORE TO ROLE ` + db_name + `_READER;
        GRANT SELECT ON ALL TABLES IN SCHEMA ` + db_name + `.OBSERVABILITY_CORE TO ROLE ` + db_name + `_READER;
        GRANT SELECT ON FUTURE TABLES IN SCHEMA ` + db_name + `.OBSERVABILITY_CORE TO ROLE ` + db_name + `_READER;
        GRANT SELECT ON ALL VIEWS IN SCHEMA ` + db_name + `.OBSERVABILITY_CORE TO ROLE ` + db_name + `_READER;
        GRANT SELECT ON FUTURE VIEWS IN SCHEMA ` + db_name + `.OBSERVABILITY_CORE TO ROLE ` + db_name + `_READER;
        -- KH COSTING DB ROLE CREATIONS
        CREATE OR REPLACE ROLE ` + account_name + `_${costing_db_name}_ADMIN;
        CREATE OR REPLACE ROLE ` + account_name + `_${costing_db_name}_DEV;
        CREATE OR REPLACE ROLE ` + account_name + `_${costing_db_name}_READER;
        GRANT ROLE ` + account_name + `_${costing_db_name}_ADMIN TO ROLE SYSADMIN;
        GRANT ROLE ` + account_name + `_${costing_db_name}_DEV TO ROLE ` + account_name + `_${costing_db_name}_ADMIN;
        GRANT ROLE ` + account_name + `_${costing_db_name}_READER TO ROLE ` + account_name + `_${costing_db_name}_ADMIN;
        -- DEV PRIVILEGES
        GRANT USAGE,MONITOR,CREATE SCHEMA ON DATABASE ` + account_name + `_${costing_db_name} TO ROLE ` + account_name + `_${costing_db_name}_DEV;
        GRANT ALL ON SCHEMA ` + account_name + `_${costing_db_name}.PUBLIC TO ROLE ` + account_name + `_${costing_db_name}_DEV;
        GRANT ALL ON ALL TABLES IN SCHEMA ` + account_name + `_${costing_db_name}.PUBLIC TO ROLE ` + account_name + `_${costing_db_name}_DEV;
        GRANT ALL ON FUTURE TABLES IN SCHEMA ` + account_name + `_${costing_db_name}.PUBLIC TO ROLE ` + account_name + `_${costing_db_name}_DEV;
        GRANT ALL ON ALL VIEWS IN SCHEMA ` + account_name + `_${costing_db_name}.PUBLIC TO ROLE ` + account_name + `_${costing_db_name}_DEV;
        GRANT ALL ON FUTURE VIEWS IN SCHEMA ` + account_name + `_${costing_db_name}.PUBLIC TO ROLE ` + account_name + `_${costing_db_name}_DEV;
        GRANT USAGE,MONITOR,CREATE SCHEMA ON DATABASE ` + account_name + `_${costing_db_name} TO ROLE ` + account_name + `_${costing_db_name}_DEV;
        GRANT ALL ON SCHEMA ` + account_name + `_${costing_db_name}.SNOWFLAKE_COST_STD TO ROLE ` + account_name + `_${costing_db_name}_DEV;
        GRANT ALL ON ALL TABLES IN SCHEMA ` + account_name + `_${costing_db_name}.SNOWFLAKE_COST_STD TO ROLE ` + account_name + `_${costing_db_name}_DEV;
        GRANT ALL ON FUTURE TABLES IN SCHEMA ` + account_name + `_${costing_db_name}.SNOWFLAKE_COST_STD TO ROLE ` + account_name + `_${costing_db_name}_DEV;
        GRANT ALL ON ALL VIEWS IN SCHEMA ` + account_name + `_${costing_db_name}.SNOWFLAKE_COST_STD TO ROLE ` + account_name + `_${costing_db_name}_DEV;
        GRANT ALL ON FUTURE VIEWS IN SCHEMA ` + account_name + `_${costing_db_name}.SNOWFLAKE_COST_STD TO ROLE ` + account_name + `_${costing_db_name}_DEV;
        -- READER PRIVILEGES
        GRANT USAGE,MONITOR ON DATABASE ` + account_name + `_${costing_db_name} TO ROLE ` + account_name + `_${costing_db_name}_READER;
        GRANT USAGE ON SCHEMA ` + account_name + `_${costing_db_name}.PUBLIC TO ROLE ` + account_name + `_${costing_db_name}_READER;
        GRANT SELECT ON ALL TABLES IN SCHEMA ` + account_name + `_${costing_db_name}.PUBLIC TO ROLE ` + account_name + `_${costing_db_name}_READER;
        GRANT SELECT ON FUTURE TABLES IN SCHEMA ` + account_name + `_${costing_db_name}.PUBLIC TO ROLE ` + account_name + `_${costing_db_name}_READER;
        GRANT SELECT ON ALL VIEWS IN SCHEMA ` + account_name + `_${costing_db_name}.PUBLIC TO ROLE ` + account_name + `_${costing_db_name}_READER;
        GRANT SELECT ON FUTURE VIEWS IN SCHEMA ` + account_name + `_${costing_db_name}.PUBLIC TO ROLE ` + account_name + `_${costing_db_name}_READER;
        GRANT USAGE,MONITOR ON DATABASE ` + account_name + `_${costing_db_name} TO ROLE ` + account_name + `_${costing_db_name}_READER;
        GRANT USAGE ON SCHEMA ` + account_name + `_${costing_db_name}.SNOWFLAKE_COST_STD TO ROLE ` + account_name + `_${costing_db_name}_READER;
        GRANT SELECT ON ALL TABLES IN SCHEMA ` + account_name + `_${costing_db_name}.SNOWFLAKE_COST_STD TO ROLE ` + account_name + `_${costing_db_name}_READER;
        GRANT SELECT ON FUTURE TABLES IN SCHEMA ` + account_name + `_${costing_db_name}.SNOWFLAKE_COST_STD TO ROLE ` + account_name + `_${costing_db_name}_READER;
        GRANT SELECT ON ALL VIEWS IN SCHEMA ` + account_name + `_${costing_db_name}.SNOWFLAKE_COST_STD TO ROLE ` + account_name + `_${costing_db_name}_READER;
        GRANT SELECT ON FUTURE VIEWS IN SCHEMA ` + account_name + `_${costing_db_name}.SNOWFLAKE_COST_STD TO ROLE ` + account_name + `_${costing_db_name}_READER;
        USE ROLE OBSERVABILITY_ADMIN;
        INSERT INTO OBSERVABILITY_MAINTENANCE.LOGGING_SCHEMA.LOGGING_TABLE
            VALUES ('CHILD_DB_SCHEMA_TABLE_STANDUP','SUCCESS', CURRENT_TIMESTAMP(), '`+account_name+`');
            RETURN 'SUCCESS';
            EXCEPTION
                WHEN STATEMENT_ERROR THEN
                LET ERROR_MESSAGE := CURRENT_TIMESTAMP()||SQLCODE || ': '||SQLERRM;
                INSERT INTO OBSERVABILITY_MAINTENANCE.LOGGING_SCHEMA.LOGGING_TABLE
                    VALUES('CHILD_DB_SCHEMA_TABLE_STANDUP','FAILED', :ERROR_MESSAGE,'`+account_name+`' );
                    USE ROLE ACCOUNTADMIN;
                    USE WAREHOUSE COMPUTE_WH;
                      
                    return object_construct('Error type','STATEMENT_ERROR',
                                            'SQLCODE', sqlcode,
                                            'SQLERRM' , sqlerrm,
                                            'SQLSTATE', sqlstate);
        END;`
      var statement6 = snowflake.createStatement({sqlText: role_creation_query});
      var result6 = statement6.execute();
      return 'SUCCESS'
      $$;