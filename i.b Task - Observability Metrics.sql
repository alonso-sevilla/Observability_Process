CALL OBSERVABILITY_SETUP.setup_procedures_v1.CHILD_KH_SNOWFLAKE_COSTING_METRICS_CREATE();
CREATE OR REPLACE PROCEDURE OBSERVABILITY_SETUP.setup_procedures_v1.CHILD_KH_SNOWFLAKE_COSTING_METRICS_CREATE()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS 
$$
var version_schema = "setup_procedures_v1";
var costing_db_name =  "KH_SNOWFLAKE_COSTING";
    
var account_query = "SELECT CURRENT_ACCOUNT()";
var statement = snowflake.createStatement({sqlText: account_query});
var account = statement.execute();
account.next();

var account_name = account.getColumnValue(1);
var query_db = `SELECT CURRENT_ACCOUNT()||'_${costing_db_name}'`;
var statement1 = snowflake.createStatement({sqlText: query_db});
var db = statement1.execute();
db.next();
var db_name = db.getColumnValue(1);

var query2 = `
    BEGIN
        CREATE TASK IF NOT EXISTS ` + db_name + `.SNOWFLAKE_COST_STD.KH_UPDATE_METRICS
        WAREHOUSE = "OBSERVABILITY_WH"
        SCHEDULE = "1440 MINUTE"
        AS
        CALL OBSERVABILITY_SETUP.${version_schema}.CHILD_${costing_db_name}_METRICS_CREATE_PROCEDURE();
        INSERT INTO OBSERVABILITY_MAINTENANCE.LOGGING_SCHEMA.LOGGING_TABLE
            VALUES ('KH_UPDATE_METRICS', 'SUCCESS', CURRENT_TIMESTAMP(), '` + account_name + `');
        RETURN 'SUCCESSFULLY CREATED ALERT AND INTEGRATION';
    EXCEPTION
    WHEN STATEMENT_ERROR THEN 
        LET ERROR_MESSAGE := CURRENT_TIMESTAMP() || SQLCODE || ': ' || SQLERRM;
        INSERT INTO OBSERVABILITY_MAINTENANCE.LOGGING_SCHEMA.LOGGING_TABLE
            VALUES ('KH_UPDATE_METRICS', 'FAILED', :ERROR_MESSAGE, '` + account_name + `');
        USE ROLE ACCOUNTADMIN;
        USE WAREHOUSE COMPUTE_WH;
        
        RETURN OBJECT_CONSTRUCT('ERROR TYPE', 'STATEMENT_ERROR',
                                'SQLCODE', SQLCODE,
                                'SQLERRM', SQLERRM,
                                'SQLSTATE', SQLSTATE);
    END;
`;

var statement3 = snowflake.createStatement({sqlText: query2});
var result2 = statement3.execute();
result2.next();

var query3 = `ALTER TASK ` + db_name + `.SNOWFLAKE_COST_STD.KH_UPDATE_METRICS RESUME;`;
var statement4 = snowflake.createStatement({sqlText: query3});
var result3 = statement4.execute();
result3.next();

return result2.getColumnValue(1).concat(" ", result3.getColumnValue(1));
$$;