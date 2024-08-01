call OBSERVABILITY_SETUP.setup_procedures_v1.CHILD_KH_SNOWFLAKE_COSTING_CREDITS_BY_QUERY();
CREATE OR REPLACE PROCEDURE OBSERVABILITY_SETUP.setup_procedures_v1.CHILD_KH_SNOWFLAKE_COSTING_CREDITS_BY_QUERY()
    RETURNS VARCHAR(16777216)
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
    AS 
    $$
        var costing_db_name = "KH_SNOWFLAKE_COSTING";
        var version_schema = "setup_procedures_v1";
        
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

        var query3 = `
        BEGIN
        CREATE TASK IF NOT EXISTS ` + db_name +`.SNOWFLAKE_COST_STD.KH_UPDATE_CREDITS_BY_QUERY
        WAREHOUSE = "OBSERVABILITY_WH"
        SCHEDULE = "300 MINUTE"
        AS
        CALL OBSERVABILITY_SETUP.${version_schema}.CHILD_${costing_db_name}_CREDITS_BY_QUERY_PROCEDURE();
        INSERT INTO OBSERVABILITY_MAINTENANCE.LOGGING_SCHEMA.LOGGING_TABLE
        VALUES ('CHILD_${costing_db_name}_CREDITS_BY_QUERY','SUCCESS', CURRENT_TIMESTAMP(), '`+account_name+`');
        RETURN 'SUCESS';
        EXCEPTION
          WHEN STATEMENT_ERROR THEN 
            LET ERROR_MESSAGE := CURRENT_TIMESTAMP()||sqlcode || ': '||sqlerrm;
            INSERT INTO OBSERVABILITY_MAINTENANCE.LOGGING_SCHEMA.LOGGING_TABLE
            VALUES('CHILD_${costing_db_name}_CREDITS_BY_QUERY','FAILED', :ERROR_MESSAGE,'`+account_name+`' );
            USE ROLE ACCOUNTADMIN;
                  USE WAREHOUSE COMPUTE_WH;

            RETURN OBJECT_CONSTRUCT('ERROR TYPE','STATEMENT_ERROR',
                                      'SQLCODE', sqlcode,
                                      'SQLERRM' , sqlerrm,
                                      'SQLSTATE', sqlstate);
        END;`
      var statement4 = snowflake.createStatement({sqlText: query3});
          var result3 = statement4.execute()
          result3.next()

      var query4 = `ALTER TASK ` + db_name + `.SNOWFLAKE_COST_STD.KH_UPDATE_CREDITS_BY_QUERY RESUME;`
      var statement5 = snowflake.createStatement({sqlText: query4});
          var result4 = statement5.execute()
          result4.next()
          return result3.getColumnValue(1).concat(" " ,result4.getColumnValue(1)) ;
    $$;