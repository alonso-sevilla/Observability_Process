call OBSERVABILITY_SETUP.SETUP_PROCEDURES_V1.child_alert_creation('alonso.sevilla@wahlclipper.com','david.nelsen@wahlclipper.com');
CREATE OR REPLACE PROCEDURE OBSERVABILITY_SETUP.SETUP_PROCEDURES_V1.child_alert_creation(INFRA_EMAIL VARCHAR, CUSTOMER_EMAIL VARCHAR)
      RETURNS STRING
      LANGUAGE JAVASCRIPT
      EXECUTE AS CALLER
    AS
    $$

    var costing_db_name = "KH_SNOWFLAKE_COSTING";
    var version_schema = "setup_procedures_v1";

    var infra_email = INFRA_EMAIL;
    var customer_email = CUSTOMER_EMAIL;

    var account_query = "SELECT CURRENT_ACCOUNT()";
    var statement = snowflake.createStatement({sqlText: account_query});
    var account = statement.execute()
    account.next();
    var account_name = account.getColumnValue(1);

    var query_db = "SELECT CURRENT_ACCOUNT()|| '_OBSERVABILITY.OBSERVABILITY_CORE.OBSERVABILITY_METRICS'";
      var statement1 = snowflake.createStatement({sqlText: query_db});
      var db = statement1.execute()
      db.next();
      var db_name = db.getColumnValue(1);

      var query_alert = "SELECT CURRENT_ACCOUNT()|| '_OBSERVABILITY'";
      var statement3 = snowflake.createStatement({sqlText: query_alert});
      var alert = statement3.execute()
      alert.next();
      var database = alert.getColumnValue(1);

      var sql_query = `
        BEGIN
        USE ROLE OBSERVABILITY_ADMIN;
        USE WAREHOUSE OBSERVABILITY_WH;
        USE DATABASE ` + database + `;
        USE SCHEMA OBSERVABILITY_CORE;
        
        CREATE OR REPLACE NOTIFICATION INTEGRATION alert_infra
        TYPE=EMAIL
        ENABLED=TRUE
        ALLOWED_RECIPIENTS=('` + infra_email + `', '` + customer_email + `');



        CREATE ALERT IF NOT EXISTS ALERT_NEW_ROWS
        WAREHOUSE = OBSERVABILITY_WH
        SCHEDULE = '1440 MINUTE'
        IF (EXISTS (
        SELECT *
        FROM ` + db_name + `
        WHERE TIMESTAMP BETWEEN SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME()
        AND SNOWFLAKE.ALERT.SCHEDULED_TIME()
        ))
        THEN call OBSERVABILITY_SETUP.${version_schema}.EMAIL_LAST_RESULTS('` + infra_email + `', '` + customer_email + `', 'New Alerts on account `+account_name+`');

        ALTER ALERT ALERT_NEW_ROWS RESUME;

        INSERT INTO OBSERVABILITY_MAINTENANCE.LOGGING_SCHEMA.LOGGING_TABLE
        VALUES ('CHILD_ALERT_CREATION','SUCCESS', CURRENT_TIMESTAMP(), '`+account_name+`');
        RETURN 'SUCCESSFULLY CREATED ALERT AND INTEGRATION';
        EXCEPTION
        WHEN STATEMENT_ERROR THEN
            RETURN SQLERRM;
                
          
        END`;
          var statement2 = snowflake.createStatement({sqlText: sql_query});
          var result = statement2.execute();
      $$;