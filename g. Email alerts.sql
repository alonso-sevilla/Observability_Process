call OBSERVABILITY_SETUP.SETUP_PROCEDURES_V1.EMAIL_LAST_RESULTS(
    'alonso.sevilla@wahlclipper.com',
    'chris.newgard@wahlclipper.com',
    'david.nelsen@wahlclipper.com',
    'srirajkumar.vasudevan@wahlclipper.com',
    'charlie.hawk@wahlclipper.com',
    'Monitoring Process - New Alerts'
);
CREATE OR REPLACE PROCEDURE OBSERVABILITY_SETUP.SETUP_PROCEDURES_V1.EMAIL_LAST_RESULTS("SEND_TO1" VARCHAR(16777216), "SEND_TO2" VARCHAR(16777216), "SEND_TO3" VARCHAR(16777216), "SEND_TO4" VARCHAR(16777216), "SEND_TO5" VARCHAR(16777216), "SUBJECT" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python','tabulate')
HANDLER = 'x'
EXECUTE AS CALLER
AS '
import snowflake

def x(session, send_to1, send_to2, send_to3, send_to4, send_to5, subject):
  session.sql("use role observability_admin").collect()
  session.sql("use warehouse observability_wh").collect()
  query_db = session.sql("SELECT CURRENT_ACCOUNT()|| ''_OBSERVABILITY.OBSERVABILITY_CORE.OBSERVABILITY_METRICS''").to_pandas().iloc[0]["CURRENT_ACCOUNT()|| ''_OBSERVABILITY.OBSERVABILITY_CORE.OBSERVABILITY_METRICS''"]
  try: 
      body = ''New alerts on account''
  except snowflake.snowpark.exceptions.SnowparkSQLException as e:
      body = ''%s\\n%s'' % (type(e), e)

  session.call(''system$send_email'', ''alert_infra'', f''{send_to1}, {send_to2}, {send_to3}, {send_to4}, {send_to5}'', subject, body)
  return ''email sent:\\n%s'' % body
';
