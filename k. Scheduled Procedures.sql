CALL OBSERVABILITY_SETUP.setup_procedures_v1.CHILD_CRON_SETUP('IE93671', 'alonso.sevilla@wahlclipper.com','chris.newgard@wahlclipper.com','david.nelsen@wahlclipper.com');
CREATE OR REPLACE PROCEDURE OBSERVABILITY_SETUP.setup_procedures_v1.CHILD_CRON_SETUP(FEDERATED_ACCOUNT_ID VARCHAR, INFRA_EMAIL VARCHAR, INFRA_EMAIL2 VARCHAR, CUSTOMER_EMAIL VARCHAR)
          returns string
          language javascript
          execute as caller
      AS
      $$
      var version_schema = "setup_procedures_v1";
      var costing_db_name = "KH_SNOWFLAKE_COSTING";
      
      var federated_account_id = FEDERATED_ACCOUNT_ID;

      var infra_email = INFRA_EMAIL;
      var infra_email2 = INFRA_EMAIL2;
      var customer_email = CUSTOMER_EMAIL;

      var harmfulKeywords = ['drop', 'create', 'alter', 'delete', 'replace', 'truncate', 'merge', ';'];
      var inputsToCheck = [federated_account_id, infra_email, infra_email2, customer_email];
      var checkInputs = inputsToCheck.join(' ').toLowerCase();

      for (var i = 0; i < harmfulKeywords.length; i++) {
        if (checkInputs.includes(harmfulKeywords[i])) {
          return 'Error: Potentially harmful SQL keyword found in inputs.';
        }
      }


      var account_query = "SELECT CURRENT_ACCOUNT()";
        var statement = snowflake.createStatement({sqlText: account_query});
        var account = statement.execute()
        account.next();
        account_name = account.getColumnValue(1);

      var sql_query_role = `CALL OBSERVABILITY_SETUP.${version_schema}.CHILD_ROLE_CREATION();`;
      var statement0 = snowflake.createStatement({sqlText: sql_query_role});
      var result0 = statement0.execute();

      var sql_query_use_role = `BEGIN 
          USE ROLE OBSERVABILITY_ADMIN;
          USE WAREHOUSE OBSERVABILITY_WH; 
          END;`;
      var statement1 = snowflake.createStatement({sqlText: sql_query_use_role});
      var result1 = statement1.execute();

      var sql_query = `
          BEGIN
          USE ROLE OBSERVABILITY_ADMIN;
          USE WAREHOUSE OBSERVABILITY_WH;
          CALL OBSERVABILITY_SETUP.${version_schema}.CHILD_DB_SCHEMA_TABLE_STANDUP();
          USE ROLE OBSERVABILITY_ADMIN;
          USE WAREHOUSE OBSERVABILITY_WH;
          CALL OBSERVABILITY_SETUP.${version_schema}.OBSERVABILITY_METRICS_CREATE_PROCEDURE();
          CALL OBSERVABILITY_SETUP.${version_schema}.CHILD_${costing_db_name}_CREDITS_BY_QUERY_PROCEDURE();
          CALL OBSERVABILITY_SETUP.${version_schema}.CHILD_${costing_db_name}_METRICS_CREATE_PROCEDURE();
          CALL OBSERVABILITY_SETUP.${version_schema}.CHILD_LONG_RUNNING_QUERY_ALERT ('` + infra_email + `', '` + infra_email2 + `', '` + customer_email + `');
          CALL OBSERVABILITY_SETUP.${version_schema}.CHILD_ALERT_CREATION('` + infra_email + `', '` + infra_email2 + `', '` + customer_email + `');
          INSERT INTO OBSERVABILITY_MAINTENANCE.LOGGING_SCHEMA.LOGGING_TABLE
          VALUES ('CHILD_CRON_SETUP','SUCCESS', CURRENT_TIMESTAMP(), '`+account_name+`');
          RETURN 'SUCCESSFULLY CREATED ALERT AND INTEGRATION';
          EXCEPTION
            WHEN STATEMENT_ERROR THEN 
              USE WAREHOUSE OBSERVABILITY_WH; 
              LET ERROR_MESSAGE := CURRENT_TIMESTAMP()||sqlcode || ': '||sqlerrm;
              INSERT INTO OBSERVABILITY_MAINTENANCE.LOGGING_SCHEMA.LOGGING_TABLE
              VALUES('CHILD_CRON_SETUP','FAILED', :ERROR_MESSAGE,'`+account_name+`' );

              RETURN OBJECT_CONSTRUCT('ERROR TYPE','STATEMENT_ERROR',
                                    'SQLCODE', sqlcode,
                                    'SQLERRM' , sqlerrm,
                                    'SQLSTATE', sqlstate);
            END;`;
      var statement2 = snowflake.createStatement({sqlText: sql_query});
      var result = statement2.execute();
      $$;
