/*==============================================================================
  SP_REGISTER_S3_BATCH - the bookkeeping part of SP_LOAD_RAW, for batches that
  Snowpipe has already loaded from s3://.../incoming/<BATCH_ID>/.
    1. registers the batch in MIG_BATCH_CONTROL as RUNNING + CURRENT
    2. removes old keyword files of the batch
    3. per source file: rows in RAW for this batch + Snowpipe's own COPY_HISTORY
       (status, rows parsed, errors, first error) -> AUDIT.MIG_LOAD_LOG
  Returns the same style of message as SP_LOAD_RAW, so the rest of the
  pipeline (and dbt's call_procedure macro) works unchanged.

      CALL AUDIT.SP_REGISTER_S3_BATCH('BATCH_002');
==============================================================================*/
USE ROLE MIG_DEVELOPER;
USE WAREHOUSE MIG_WH;
USE DATABASE HC_MIGRATION_DB;

CREATE OR REPLACE PROCEDURE AUDIT.SP_REGISTER_S3_BATCH(P_BATCH_ID VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_tbl        VARCHAR;
    v_file       VARCHAR;
    v_fqn        VARCHAR;
    v_rows       INTEGER DEFAULT 0;
    v_pipe_state VARCHAR;
    v_parsed     INTEGER;
    v_err_seen   INTEGER;
    v_first_err  VARCHAR;
    v_status     VARCHAR;
    v_msg        VARCHAR;
    v_sql        VARCHAR;
    v_summary    VARCHAR DEFAULT '';
    v_errors     INTEGER DEFAULT 0;
    c_files CURSOR FOR
        SELECT RAW_TABLE, FILE_NAME
        FROM HC_MIGRATION_DB.AUDIT.MIG_SOURCE_FILE_CONFIG
        WHERE IS_ACTIVE
        ORDER BY LOAD_SEQ;
BEGIN
    MERGE INTO HC_MIGRATION_DB.AUDIT.MIG_BATCH_CONTROL t
    USING (SELECT :P_BATCH_ID AS BATCH_ID) s
       ON t.BATCH_ID = s.BATCH_ID
    WHEN MATCHED THEN UPDATE SET STATUS = 'RUNNING', STARTED_AT = CURRENT_TIMESTAMP(), ENDED_AT = NULL
    WHEN NOT MATCHED THEN INSERT (BATCH_ID, BATCH_DESC, STATUS, STARTED_AT)
                          VALUES (s.BATCH_ID, 'Loaded from S3 by Snowpipe', 'RUNNING', CURRENT_TIMESTAMP());

    UPDATE HC_MIGRATION_DB.AUDIT.MIG_BATCH_CONTROL
       SET IS_CURRENT = (BATCH_ID = :P_BATCH_ID);

    v_sql := 'REMOVE @HC_MIGRATION_DB.AUDIT.KEYWORD_STAGE/' || P_BATCH_ID || '/';
    EXECUTE IMMEDIATE :v_sql;

    FOR f IN c_files DO
        v_tbl  := f.RAW_TABLE;
        v_file := f.FILE_NAME;
        v_fqn  := 'HC_MIGRATION_DB.RAW.' || v_tbl;

        SELECT COUNT(*) INTO :v_rows FROM IDENTIFIER(:v_fqn) WHERE BATCH_ID = :P_BATCH_ID;

        -- what Snowpipe itself reported for this file (latest attempt)
        v_pipe_state := NULL; v_parsed := NULL; v_err_seen := NULL; v_first_err := NULL;
        SELECT STATUS, ROW_PARSED, ERROR_COUNT, FIRST_ERROR_MESSAGE
          INTO :v_pipe_state, :v_parsed, :v_err_seen, :v_first_err
          FROM TABLE(HC_MIGRATION_DB.INFORMATION_SCHEMA.COPY_HISTORY(
                     TABLE_NAME => :v_fqn, START_TIME => DATEADD('day', -14, CURRENT_TIMESTAMP())))
         WHERE FILE_NAME = :P_BATCH_ID || '/' || :v_file          -- relative to the pipe's incoming/ folder
           AND PIPE_NAME IS NOT NULL
         ORDER BY LAST_LOAD_TIME DESC
         LIMIT 1;

        IF (v_rows > 0 AND COALESCE(v_err_seen, 0) = 0) THEN
            v_status := 'SUCCESS';
            v_msg    := 'Loaded ' || v_file || ' by Snowpipe';
        ELSEIF (v_rows > 0) THEN
            v_status := 'WARNING';
            v_msg    := 'Loaded ' || v_file || ' by Snowpipe, ' || v_err_seen || ' row(s) rejected: ' || COALESCE(v_first_err, '');
        ELSE
            v_status := 'WARNING';
            v_msg    := 'No rows for ' || v_file || ' - Snowpipe status: ' || COALESCE(v_pipe_state, 'file not received') ||
                        ' (expected s3://<your-landing-bucket>/incoming/' || P_BATCH_ID || '/' || v_file || ')';
        END IF;

        INSERT INTO HC_MIGRATION_DB.AUDIT.MIG_LOAD_LOG (BATCH_ID, STEP_NAME, OBJECT_NAME, ROW_COUNT, STATUS, MESSAGE)
        VALUES (:P_BATCH_ID, 'LOAD_RAW', :v_tbl, :v_rows, :v_status, :v_msg);

        v_summary := v_summary || v_tbl || '=' || v_rows || ' (' || v_status || ') ';
    END FOR;

    RETURN 'LOAD_RAW complete: ' || v_summary;
END;
$$;
