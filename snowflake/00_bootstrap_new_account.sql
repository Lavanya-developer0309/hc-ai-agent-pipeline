/* ============================================================================
   ONE-TIME ADMIN SETUP for the NEW Snowflake account (AWS Asia Pacific - Sydney)
   Run once in Snowsight as ACCOUNTADMIN: paste everything, then Cmd+Shift+Return.
   1) roles, warehouse (XSMALL, 60s auto-suspend, 10-credit monthly cap), database, schemas
   2) MIG_DBT_SVC service user with the existing key pair (public key below)
   3) S3 storage integration to the landing bucket (same AWS region now)
   Everything else (tables, stages, dbt, profiling) is run by Claude as MIG_DEVELOPER.
   ============================================================================ */

USE ROLE ACCOUNTADMIN;

-------------------------------------------------------------------------------
-- 1. Roles + hierarchy
-------------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS MIG_DEVELOPER COMMENT = 'Migration developer - builds and runs the pipeline';
CREATE ROLE IF NOT EXISTS MIG_QA        COMMENT = 'Migration QA - read-only on all layers, PHI masked';
CREATE ROLE IF NOT EXISTS MIG_ANALYST   COMMENT = 'Business analyst - read-only on target + audit, PHI masked';

GRANT ROLE MIG_ANALYST   TO ROLE MIG_QA;
GRANT ROLE MIG_QA        TO ROLE MIG_DEVELOPER;
GRANT ROLE MIG_DEVELOPER TO ROLE SYSADMIN;      -- best practice: custom roles roll up to SYSADMIN

-- give all three roles to the person running this script, so you can switch
-- roles in Snowsight and demo the masking policies
SET MY_USER = CURRENT_USER();
GRANT ROLE MIG_DEVELOPER TO USER IDENTIFIER($MY_USER);
GRANT ROLE MIG_QA        TO USER IDENTIFIER($MY_USER);
GRANT ROLE MIG_ANALYST   TO USER IDENTIFIER($MY_USER);

-------------------------------------------------------------------------------
-- 2. Warehouse (compute) - smallest size, suspends after 60 seconds idle
-------------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS MIG_WH
    WAREHOUSE_SIZE      = 'XSMALL'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT             = 'Compute for the healthcare data migration project';

-- cost guard-rail: stop the warehouse if it burns 10 credits in a month
CREATE RESOURCE MONITOR IF NOT EXISTS MIG_RM
    WITH CREDIT_QUOTA = 10
         FREQUENCY = MONTHLY
         START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 80  PERCENT DO NOTIFY
             ON 100 PERCENT DO SUSPEND;
ALTER WAREHOUSE MIG_WH SET RESOURCE_MONITOR = MIG_RM;

GRANT USAGE ON WAREHOUSE MIG_WH TO ROLE MIG_ANALYST;           -- inherited by QA + DEV
GRANT OPERATE, MONITOR ON WAREHOUSE MIG_WH TO ROLE MIG_DEVELOPER;

-- account-level privileges the developer role needs
GRANT CREATE DATABASE ON ACCOUNT TO ROLE MIG_DEVELOPER;
GRANT EXECUTE TASK    ON ACCOUNT TO ROLE MIG_DEVELOPER;        -- Snowflake Tasks (orchestration)

-------------------------------------------------------------------------------
-- 3. Database + schemas (owned by MIG_DEVELOPER)
-------------------------------------------------------------------------------
USE ROLE MIG_DEVELOPER;
USE WAREHOUSE MIG_WH;

CREATE DATABASE IF NOT EXISTS HC_MIGRATION_DB
    DATA_RETENTION_TIME_IN_DAYS = 1      -- Time Travel window (raise up to 90 on Enterprise edition)
    COMMENT = 'Healthcare payer data migration - legacy extracts to Facets-like target';

USE DATABASE HC_MIGRATION_DB;

CREATE TRANSIENT SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Landing zone: source extracts as-is (transient = no fail-safe cost, data is re-loadable)';
CREATE SCHEMA IF NOT EXISTS REF            COMMENT = 'Crosswalks / config values (dbt seeds)';
CREATE SCHEMA IF NOT EXISTS STAGING        COMMENT = 'dbt staging views - cleansed and typed source';
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE   COMMENT = 'dbt intermediate - crosswalks, reject reasons, contrived keys';
CREATE SCHEMA IF NOT EXISTS FACETS_STG     COMMENT = 'Facets-lookalike tables built by dbt (validated before load)';
CREATE SCHEMA IF NOT EXISTS KEYWORD        COMMENT = 'Keyword (load file) records built by dbt';
CREATE SCHEMA IF NOT EXISTS FACETS_TGT     COMMENT = 'Target Facets-like environment - loaded only from keyword files';
CREATE SCHEMA IF NOT EXISTS AUDIT          COMMENT = 'Control, logging, profiling, scorecard, procedures, stages';
CREATE SCHEMA IF NOT EXISTS GOVERNANCE     COMMENT = 'Masking policies for PHI / PII';
CREATE SCHEMA IF NOT EXISTS DBT_TEST_AUDIT COMMENT = 'Failing rows captured by dbt tests (store_failures)';

-------------------------------------------------------------------------------
-- 4. Read access for QA and ANALYST (future grants cover tables dbt creates later)
-------------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

GRANT USAGE ON DATABASE HC_MIGRATION_DB TO ROLE MIG_ANALYST;

-- MIG_QA: every schema
GRANT USAGE ON ALL SCHEMAS IN DATABASE HC_MIGRATION_DB                TO ROLE MIG_QA;
GRANT SELECT ON ALL TABLES    IN DATABASE HC_MIGRATION_DB             TO ROLE MIG_QA;
GRANT SELECT ON ALL VIEWS     IN DATABASE HC_MIGRATION_DB             TO ROLE MIG_QA;
GRANT SELECT ON FUTURE TABLES IN DATABASE HC_MIGRATION_DB             TO ROLE MIG_QA;
GRANT SELECT ON FUTURE VIEWS  IN DATABASE HC_MIGRATION_DB             TO ROLE MIG_QA;

-- MIG_ANALYST: target + audit only
-- (schema-level future grants take precedence over database-level ones for that
--  schema; MIG_QA still gets these two schemas because it inherits MIG_ANALYST)
GRANT USAGE ON SCHEMA HC_MIGRATION_DB.FACETS_TGT                      TO ROLE MIG_ANALYST;
GRANT USAGE ON SCHEMA HC_MIGRATION_DB.AUDIT                           TO ROLE MIG_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA HC_MIGRATION_DB.FACETS_TGT    TO ROLE MIG_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA HC_MIGRATION_DB.AUDIT         TO ROLE MIG_ANALYST;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA HC_MIGRATION_DB.AUDIT         TO ROLE MIG_ANALYST;

USE ROLE MIG_DEVELOPER;
SHOW SCHEMAS IN DATABASE HC_MIGRATION_DB;

-------------------------------------------------------------------------------
-- 5. Service user for dbt / pipeline / profiling tool (key-pair, no password)
-------------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
CREATE USER IF NOT EXISTS MIG_DBT_SVC
    TYPE              = SERVICE              -- no password, no MFA, cannot use the UI
    DEFAULT_ROLE      = MIG_DEVELOPER
    DEFAULT_WAREHOUSE = MIG_WH
    DEFAULT_NAMESPACE = HC_MIGRATION_DB.STAGING
    COMMENT           = 'dbt / pipeline service account for the migration project';

GRANT ROLE MIG_DEVELOPER TO USER MIG_DBT_SVC;
ALTER USER MIG_DBT_SVC SET RSA_PUBLIC_KEY = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAwRsObwd7/KrGRfNpgUchowwLemkuG3RFSe3adkXFas6XctrJQWX89El10N7rnrRUPedn+TUYb0F0l5L5fcsKPJEyGfQQACuvDAQhjL8Tx/s9UY7N/bg075fU8LgxeoXyOMuMZMK1N+/QV91p9cMGSeRTGjM/3FwTQ7tAW7qWYsy56RNETmsJgL9qOkxYrpv2sU98vT2COkX5QAJr14hPJeBCucTpzv3dbSVRTYbCdpj70GZKLapUftJvLmz1mOHiGJpa3krghKnpLBMBkq6XN1c4EIhLdox8Nlvk68h8MLYWf5clqH6d/pQN7+RQHOp4YVD0wcAwufPO7tCRFuYHfwIDAQAB';

-------------------------------------------------------------------------------
-- 6. S3 landing bucket (AWS Sydney) - storage integration
-------------------------------------------------------------------------------
CREATE STORAGE INTEGRATION IF NOT EXISTS S3_HC_LANDING
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<aws-account-id>:role/snowflake-hc-landing-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<your-landing-bucket>/');
GRANT USAGE ON INTEGRATION S3_HC_LANDING TO ROLE MIG_DEVELOPER;

SELECT 'Setup complete - tell Claude done' AS STATUS, CURRENT_ACCOUNT() AS ACCOUNT_LOCATOR, CURRENT_REGION() AS REGION;
