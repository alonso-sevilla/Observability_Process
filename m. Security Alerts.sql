CALL OBSERVABILITY_SETUP.setup_procedures_v1.CHILD_SECURITY_ALERT_CREATION(
'alonso.sevilla@wahlclipper.com',
'chris.newgard@wahlclipper.com',
'david.nelsen@wahlclipper.com',
'srirajkumar.vasudevan@wahlclipper.com',
'charlie.hawk@wahlclipper.com');
CREATE OR REPLACE PROCEDURE OBSERVABILITY_SETUP.setup_procedures_v1.CHILD_SECURITY_ALERT_CREATION(
INFRA_EMAIL VARCHAR, 
INFRA_EMAIL2 VARCHAR, 
CUSTOMER_EMAIL VARCHAR, 
CUSTOMER_EMAIL2 VARCHAR, 
CUSTOMER_EMAIL3 VARCHAR)
      RETURNS STRING
      LANGUAGE JAVASCRIPT
      EXECUTE AS CALLER
    AS
    $$
    var infra_email = INFRA_EMAIL;
    var infra_email2 = INFRA_EMAIL2;
  
    var customer_email = CUSTOMER_EMAIL;
    var customer_email2 = CUSTOMER_EMAIL2;
    var customer_email3 = CUSTOMER_EMAIL3;

    var account_query = "SELECT CURRENT_ACCOUNT()";
    var statement = snowflake.createStatement({sqlText: account_query});
    var account = statement.execute()
    account.next();
    account_name = account.getColumnValue(1);

    var query_db = "SELECT CURRENT_ACCOUNT()|| '_OBSERVABILITY.OBSERVABILITY_CORE.OBSERVABILITY_METRICS'";
    var statement1 = snowflake.createStatement({sqlText: query_db});
    var db = statement1.execute()
    db.next();
    db_name = db.getColumnValue(1);

    var query_alert = "SELECT CURRENT_ACCOUNT()|| '_OBSERVABILITY'";
    var statement3 = snowflake.createStatement({sqlText: query_alert});
    var alert = statement3.execute()
    alert.next();
    database = alert.getColumnValue(1);

    var service_account_alert = `
      BEGIN
      USE DATABASE ` + database + `;
      USE ROLE OBSERVABILITY_ADMIN;
      USE WAREHOUSE OBSERVABILITY_WH;
      USE SCHEMA OBSERVABILITY_CORE;

      CREATE OR REPLACE ALERT  SERVICE_ACCOUNT_LOGIN
      WAREHOUSE = OBSERVABILITY_WH
      SCHEDULE = '1440 MINUTE'
      IF (EXISTS (
      SELECT A.GRANTEE_NAME,  B.FIRST_NAME, B.LAST_NAME, B.EMAIL, B.DISABLED, B.DEFAULT_WAREHOUSE, B.DEFAULT_NAMESPACE, B.DEFAULT_ROLE, B.LAST_SUCCESS_LOGIN
      FROM "SNOWFLAKE"."ACCOUNT_USAGE"."GRANTS_TO_USERS" A, 
      "SNOWFLAKE"."ACCOUNT_USAGE"."USERS" B
      WHERE
      A.GRANTEE_NAME LIKE '%_USER' AND
      A.GRANTEE_NAME = B.LOGIN_NAME AND
      A.DELETED_ON IS NULL AND
      B.DELETED_ON IS NULL AND
      B.LAST_SUCCESS_LOGIN BETWEEN SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME()
      AND SNOWFLAKE.ALERT.SCHEDULED_TIME()
      GROUP BY 1,2,3,4,5,6,7,8,9
      ORDER BY 2
      ))
      THEN BEGIN 
        CALL SYSTEM$SEND_EMAIL('alert_infra', '` + infra_email + `, ` + infra_email2 + `, ` + customer_email + `, ` + customer_email2 + `, ` + customer_email3 + `', 'New Security Alert on account `+account_name+`', 'someone logged in with accountadmin service role'); 
        INSERT INTO `+ db_name+` (ACCOUNT, METRICNAME, SERVICEACCOUNT, TIMESTAMP ) 
        SELECT '`+account_name+`', 'SERVICE_ACCOUNT_LOGIN' ,'someone logged in with accountadmin service role: ' || A.GRANTEE_NAME, CURRENT_TIMESTAMP()
        FROM "SNOWFLAKE"."ACCOUNT_USAGE"."GRANTS_TO_USERS" A, 
        "SNOWFLAKE"."ACCOUNT_USAGE"."USERS" B
        WHERE
        A.GRANTEE_NAME LIKE '%_USER' AND
        A.GRANTEE_NAME = B.LOGIN_NAME AND
        A.DELETED_ON IS NULL AND
        B.DELETED_ON IS NULL AND
        B.LAST_SUCCESS_LOGIN BETWEEN SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME()
        AND SNOWFLAKE.ALERT.SCHEDULED_TIME()
        GROUP BY 1,2,3,4,5,6,7,8,9
        ORDER BY 2; 
      END;

      ALTER ALERT SERVICE_ACCOUNT_LOGIN RESUME; 

      RETURN 'SUCCESSFULLY CREATED ALERT AND INTEGRATION';
      INSERT INTO OBSERVABILITY_MAINTENANCE.LOGGING_SCHEMA.LOGGING_TABLE
      VALUES ('CHILD_SECURITY_ALERT_CREATION','SUCCESS', CURRENT_TIMESTAMP(), '`+account_name+`');
      RETURN 'SUCCESS';
      EXCEPTION
        WHEN STATEMENT_ERROR THEN
          LET ERROR_MESSAGE := CURRENT_TIMESTAMP()||SQLCODE || ': '||SQLERRM;
          INSERT INTO OBSERVABILITY_MAINTENANCE.LOGGING_SCHEMA.LOGGING_TABLE
          VALUES('CHILD_SECURITY_ALERT_CREATION','FAILED', :ERROR_MESSAGE,'`+account_name+`' );
          USE ROLE ACCOUNTADMIN;
          USE WAREHOUSE COMPUTE_WH;
          
          RETURN OBJECT_CONSTRUCT('ERROR TYPE','STATEMENT_ERROR',
                                      'SQLCODE', sqlcode,
                                      'SQLERRM' , sqlerrm,
                                      'SQLSTATE', sqlstate);
      end`;
    var service_account_alert_creation = snowflake.createStatement({sqlText: service_account_alert});
    var result = service_account_alert_creation.execute();


    var admin_login_alert = `
      BEGIN
      USE DATABASE ` + database + `;
      USE ROLE OBSERVABILITY_ADMIN;
      USE WAREHOUSE OBSERVABILITY_WH;
      USE SCHEMA OBSERVABILITY_CORE;

      CREATE OR REPLACE ALERT ADMIN_LOGIN_ALERT
      WAREHOUSE = OBSERVABILITY_WH
      SCHEDULE = '1440 MINUTE'
      IF (EXISTS (
        SELECT A.GRANTEE_NAME, A.ROLE, B.FIRST_NAME, B.LAST_NAME, B.EMAIL, B.DISABLED, B.DEFAULT_WAREHOUSE, B.DEFAULT_NAMESPACE, B.DEFAULT_ROLE, C.COMMENT,B.LAST_SUCCESS_LOGIN
        FROM "SNOWFLAKE"."ACCOUNT_USAGE"."GRANTS_TO_USERS" A, 
        "SNOWFLAKE"."ACCOUNT_USAGE"."USERS" B, 
        "SNOWFLAKE"."ACCOUNT_USAGE"."ROLES" C 
        WHERE
        A.GRANTEE_NAME = B.LOGIN_NAME AND
        A.ROLE = C.NAME AND
        A.DELETED_ON IS NULL AND
        B.DELETED_ON IS NULL  AND
        C.DELETED_ON IS NULL AND
        A.ROLE IN('ACCOUNTADMIN', 'SYSADMIN', 'SECURITYADMIN') AND
        B.LAST_SUCCESS_LOGIN BETWEEN SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME()
        AND SNOWFLAKE.ALERT.SCHEDULED_TIME()
        ORDER BY 2
        ))
      THEN BEGIN 
        CALL SYSTEM$SEND_EMAIL('alert_infra', '` + infra_email + `, ` + infra_email2 + `, ` + customer_email + `, ` + customer_email2 + `, ` + customer_email3 + `', 'New Security Alert on account `+account_name+`', 'someone logged in with an accountadmin, sysadmin or securityadmin role'); 
        INSERT INTO `+ db_name+` (ACCOUNT, METRICNAME, ADMINLOGIN, TIMESTAMP ) 
        SELECT '`+account_name+`', 'ADMIN_LOGIN_ALERT' ,'someone logged in with an accountadmin, sysadmin or securityadmin role:  ' || A.GRANTEE_NAME, CURRENT_TIMESTAMP()
        FROM "SNOWFLAKE"."ACCOUNT_USAGE"."GRANTS_TO_USERS" A, 
        "SNOWFLAKE"."ACCOUNT_USAGE"."USERS" B, 
        "SNOWFLAKE"."ACCOUNT_USAGE"."ROLES" C 
        WHERE
        A.GRANTEE_NAME = B.LOGIN_NAME AND
        A.ROLE = C.NAME AND
        A.DELETED_ON IS NULL AND
        B.DELETED_ON IS NULL  AND
        C.DELETED_ON IS NULL AND
        A.ROLE IN('ACCOUNTADMIN', 'SYSADMIN', 'SECURITYADMIN') AND
        B.LAST_SUCCESS_LOGIN BETWEEN SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME()
        AND SNOWFLAKE.ALERT.SCHEDULED_TIME()
        ORDER BY 2; 
      END;

      ALTER ALERT ADMIN_LOGIN_ALERT RESUME;

      RETURN 'SUCCESSFULLY CREATED ALERT AND INTEGRATION';
      INSERT INTO OBSERVABILITY_MAINTENANCE.LOGGING_SCHEMA.LOGGING_TABLE
      VALUES ('CHILD_SECURITY_ALERT_CREATION','SUCCESS', CURRENT_TIMESTAMP(), '`+account_name+`');
      RETURN 'SUCCESS';
      EXCEPTION
        WHEN STATEMENT_ERROR THEN
          LET ERROR_MESSAGE := CURRENT_TIMESTAMP()||SQLCODE || ': '||SQLERRM;
          INSERT INTO OBSERVABILITY_MAINTENANCE.LOGGING_SCHEMA.LOGGING_TABLE
          VALUES('CHILD_SECURITY_ALERT_CREATION','FAILED', :ERROR_MESSAGE,'`+account_name+`' );
          USE ROLE ACCOUNTADMIN;
          USE WAREHOUSE COMPUTE_WH;
          
          RETURN OBJECT_CONSTRUCT('ERROR TYPE','STATEMENT_ERROR',
                                      'SQLCODE', sqlcode,
                                      'SQLERRM' , sqlerrm,
                                      'SQLSTATE', sqlstate);
      end`;
    var admin_login_alert_creation = snowflake.createStatement({sqlText: admin_login_alert});
    var result = admin_login_alert_creation.execute();

    var unused_role_alert = `
      BEGIN
      USE DATABASE ` + database + `;
      USE ROLE OBSERVABILITY_ADMIN;
      USE WAREHOUSE OBSERVABILITY_WH;
      USE SCHEMA OBSERVABILITY_CORE;

      CREATE OR REPLACE ALERT UNUSED_ROLE_ALERT
      WAREHOUSE = OBSERVABILITY_WH
      SCHEDULE = 'USING CRON 0 5 * * * UTC'
      IF (EXISTS (
      SELECT ROLE_NAME
      FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
      GROUP BY 1
      HAVING MAX(END_TIME) < DATEADD(MONTH, -6, CURRENT_TIMESTAMP)
      ))
      THEN BEGIN 
        CALL SYSTEM$SEND_EMAIL('alert_infra', '` + infra_email + `, ` + infra_email2 + `, ` + customer_email + `, ` + customer_email2 + `, ` + customer_email3 + `', 'New Security Alert on account `+account_name+`', 'a role has not been used in the last 6 month, please look into this'); 
        INSERT INTO `+ db_name+` (ACCOUNT, METRICNAME, UNUSEDROLE, TIMESTAMP ) 
        SELECT '`+account_name+`', 'UNUSED_ROLE_ALERT' ,'a role has not been used in the last 6 month, please look into this  ' || ROLE_NAME, CURRENT_TIMESTAMP()
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        GROUP BY 1,2,3,4
        HAVING MAX(END_TIME) > DATEADD(MONTH, -6, CURRENT_TIMESTAMP); 
        END;

      ALTER ALERT UNUSED_ROLE_ALERT RESUME;

      RETURN 'SUCCESSFULLY CREATED ALERT AND INTEGRATION';
      INSERT INTO OBSERVABILITY_MAINTENANCE.LOGGING_SCHEMA.LOGGING_TABLE
      VALUES ('CHILD_SECURITY_ALERT_CREATION','SUCCESS', CURRENT_TIMESTAMP(), '`+account_name+`');
      RETURN 'SUCCESS';
      EXCEPTION
        WHEN statement_error THEN
          LET ERROR_MESSAGE := CURRENT_TIMESTAMP()||SQLCODE || ': '||SQLERRM;
          INSERT INTO OBSERVABILITY_MAINTENANCE.LOGGING_SCHEMA.LOGGING_TABLE
          VALUES('CHILD_SECURITY_ALERT_CREATION','FAILED', :ERROR_MESSAGE,'`+account_name+`' );
          USE ROLE ACCOUNTADMIN;
          USE WAREHOUSE COMPUTE_WH;
          RETURN OBJECT_CONSTRUCT('ERROR TYPE','STATEMENT_ERROR',
                                  'SQLCODE', sqlcode,
                                  'SQLERRM' , sqlerrm,
                                  'SQLSTATE', sqlstate);
          END`;
    var unused_role_alert_creation = snowflake.createStatement({sqlText: unused_role_alert});
    var result = unused_role_alert_creation.execute();

    var not_allowed_query = `
      BEGIN
      USE DATABASE ` + database + `;
      USE ROLE OBSERVABILITY_ADMIN;
      USE WAREHOUSE OBSERVABILITY_WH;
      USE SCHEMA OBSERVABILITY_CORE;

      CREATE OR REPLACE ALERT NOT_ALLOWED_ADMIN_LOGIN_ALERT
        WAREHOUSE = OBSERVABILITY_WH
        SCHEDULE = '1440 MINUTE'
        IF (EXISTS (
          SELECT A.GRANTEE_NAME, A.ROLE, B.FIRST_NAME, B.LAST_NAME, B.EMAIL, B.DISABLED, B.DEFAULT_WAREHOUSE, B.DEFAULT_NAMESPACE, B.DEFAULT_ROLE, C.COMMENT,B.LAST_SUCCESS_LOGIN
          FROM "SNOWFLAKE"."ACCOUNT_USAGE"."GRANTS_TO_USERS" A, 
          "SNOWFLAKE"."ACCOUNT_USAGE"."USERS" B, 
          "SNOWFLAKE"."ACCOUNT_USAGE"."ROLES" C 
          WHERE
          A.GRANTEE_NAME = B.LOGIN_NAME AND
          A.ROLE = C.NAME AND
          A.DELETED_ON IS NULL AND
          B.DELETED_ON IS NULL  AND
          C.DELETED_ON IS NULL AND
          A.ROLE IN('ACCOUNTADMIN', 'SYSADMIN', 'SECURITYADMIN') AND
          NOT EXISTS (
            SELECT USERNAME 
            FROM `+database +`.OBSERVABILITY_CORE.ALLOWED_USERADMINS AU 
            WHERE A.GRANTEE_NAME = AU.USERNAME ) AND
          B.LAST_SUCCESS_LOGIN BETWEEN SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME()
          AND SNOWFLAKE.ALERT.SCHEDULED_TIME()
          ORDER BY 2
          ))
        THEN BEGIN 
          CALL SYSTEM$SEND_EMAIL('alert_infra', '` + infra_email + `, ` + infra_email2 + `, ` + customer_email + `, ` + customer_email2 + `, ` + customer_email3 + `', 'New Security Alert on account `+account_name+`', 'someone that is not in the allowed list logged in with an accountadmin, sysadmin or securityadmin role'); 
          INSERT INTO `+ db_name+` (ACCOUNT, METRICNAME, NOTALLOWEDUSER, TIMESTAMP ) 
          SELECT '`+account_name+`', 'NOT_ALLOWED_ADMIN_LOGIN_ALERT' ,'someone that is not in the allowed list logged in with an accountadmin, sysadmin or securityadmin role:  ' || A.GRANTEE_NAME, CURRENT_TIMESTAMP()
          FROM "SNOWFLAKE"."ACCOUNT_USAGE"."GRANTS_TO_USERS" A, 
          "SNOWFLAKE"."ACCOUNT_USAGE"."USERS" B, 
          "SNOWFLAKE"."ACCOUNT_USAGE"."ROLES" C 
          WHERE
          A.GRANTEE_NAME = B.LOGIN_NAME AND
          A.ROLE = C.NAME AND
          A.DELETED_ON IS NULL AND
          B.DELETED_ON IS NULL  AND
          C.DELETED_ON IS NULL AND
          A.ROLE IN('ACCOUNTADMIN', 'SYSADMIN', 'SECURITYADMIN') AND
          NOT EXISTS (
            SELECT USERNAME 
            FROM `+database +`.OBSERVABILITY_CORE.ALLOWED_USERADMINS AU 
            WHERE A.GRANTEE_NAME = AU.USERNAME ) AND
          B.LAST_SUCCESS_LOGIN BETWEEN SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME()
          AND SNOWFLAKE.ALERT.SCHEDULED_TIME()
          ORDER BY 2; 
          END;
      
      ALTER ALERT NOT_ALLOWED_ADMIN_LOGIN_ALERT RESUME;

      RETURN 'SUCCESS';
      INSERT INTO OBSERVABILITY_MAINTENANCE.LOGGING_SCHEMA.LOGGING_TABLE
      VALUES ('CHILD_SECURITY_ALERT_CREATION','SUCCESS', CURRENT_TIMESTAMP(), '`+account_name+`');
      RETURN 'SUCCESS';
      EXCEPTION
        WHEN STATEMENT_ERROR THEN
          LET ERROR_MESSAGE := CURRENT_TIMESTAMP()||SQLCODE || ': '||SQLERRM;
          INSERT INTO OBSERVABILITY_MAINTENANCE.LOGGING_SCHEMA.LOGGING_TABLE
          VALUES('CHILD_SECURITY_ALERT_CREATION','FAILED', :ERROR_MESSAGE,'`+account_name+`' );
          USE ROLE ACCOUNTADMIN;
          USE WAREHOUSE COMPUTE_WH;
          RETURN OBJECT_CONSTRUCT('ERROR TYPE','STATEMENT_ERROR',
                                      'SQLCODE', sqlcode,
                                      'SQLERRM' , sqlerrm,
                                      'SQLSTATE', sqlstate);
      END`;
    var not_allowed_alert_creation = snowflake.createStatement({sqlText: not_allowed_query});
    var result = not_allowed_alert_creation.execute();
    result.next();
    return result.getColumnValue(1);
    $$;
