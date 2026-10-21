-- Run ONCE in Snowsight as ACCOUNTADMIN (Projects -> Worksheets -> + SQL Worksheet).
-- Free: a storage integration is just a trust link, it uses no credits.
USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION IF NOT EXISTS S3_HC_LANDING
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<aws-account-id>:role/snowflake-hc-landing-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<your-landing-bucket>/')
  COMMENT = 'Landing bucket (AWS Sydney) for the healthcare migration pipeline';

-- let the pipeline's role (used by the tool, dbt and Airflow) use it
GRANT USAGE ON INTEGRATION S3_HC_LANDING TO ROLE MIG_DEVELOPER;

-- just to see it worked; Claude reads these values itself afterwards
DESC INTEGRATION S3_HC_LANDING;
