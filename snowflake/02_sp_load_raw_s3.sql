/*==============================================================================
  SP_LOAD_RAW_S3 - same as AUDIT.SP_LOAD_RAW, but reads the batch from AWS S3:
      s3://<your-landing-bucket>/incoming/<BATCH_ID>/<FILE_NAME>
  through the external stage RAW.STG_S3_LANDING.
  Generated from the migration project's sql/01_raw_load/02_sp_load_raw.sql
  (only the stage path and messages differ), so both loads behave the same.

      CALL AUDIT.SP_LOAD_RAW_S3('BATCH_002');
==============================================================================*/
USE ROLE MIG_DEVELOPER;
USE WAREHOUSE MIG_WH;
USE DATABASE HC_MIGRATION_DB;

CREATE OR REPLACE PROCEDURE AUDIT.SP_LOAD_RAW_S3(P_BATCH_ID VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_tbl        VARCHAR;
    v_file       VARCHAR;
    v_fqn        VARCHAR;
    v_col_count  INTEGER DEFAULT 0;
    v_select     VARCHAR;
    v_sql        VARCHAR;
    v_rows       INTEGER DEFAULT 0;
    v_status     VARCHAR;
    v_msg        VARCHAR;
    v_summary    VARCHAR DEFAULT '';
    v_errors     INTEGER DEFAULT 0;
    c_files CURSOR FOR
        SELECT RAW_TABLE, FILE_NAME
        FROM HC_MIGRATION_DB.AUDIT.MIG_SOURCE_FILE_CONFIG
        WHERE IS_ACTIVE
        ORDER BY LOAD_SEQ;
BEGIN
    -- register / start the batch and make it the current one
    MERGE INTO HC_MIGRATION_DB.AUDIT.MIG_BATCH_CONTROL t
    USING (SELECT :P_BATCH_ID AS BATCH_ID) s
       ON t.BATCH_ID = s.BATCH_ID
    WHEN MATCHED THEN UPDATE SET STATUS = 'RUNNING', STARTED_AT = CURRENT_TIMESTAMP(), ENDED_AT = NULL
    WHEN NOT MATCHED THEN INSERT (BATCH_ID, BATCH_DESC, STATUS, STARTED_AT)
                          VALUES (s.BATCH_ID, 'Registered by SP_LOAD_RAW_S3', 'RUNNING', CURRENT_TIMESTAMP());

    UPDATE HC_MIGRATION_DB.AUDIT.MIG_BATCH_CONTROL
       SET IS_CURRENT = (BATCH_ID = :P_BATCH_ID);

    -- a new load starts a new cycle: old keyword files of this batch are no longer valid
    v_sql := 'REMOVE @HC_MIGRATION_DB.AUDIT.KEYWORD_STAGE/' || P_BATCH_ID || '/';
    EXECUTE IMMEDIATE :v_sql;

    FOR f IN c_files DO
        v_tbl  := f.RAW_TABLE;
        v_file := f.FILE_NAME;
        v_fqn  := 'HC_MIGRATION_DB.RAW.' || v_tbl;

        BEGIN
            -- number of data columns = all columns minus the 4 lineage columns
            SELECT COUNT(*) INTO :v_col_count
            FROM HC_MIGRATION_DB.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'RAW'
              AND TABLE_NAME   = :v_tbl
              AND COLUMN_NAME NOT IN ('BATCH_ID', 'SRC_FILE_NAME', 'SRC_ROW_NUM', 'LOAD_TS');

            -- build "$1, $2, ... $n" for the COPY transformation
            v_select := '';
            FOR i IN 1 TO v_col_count DO
                v_select := v_select || '$' || i || ', ';
            END FOR;

            -- 1. make the step re-runnable
            DELETE FROM IDENTIFIER(:v_fqn) WHERE BATCH_ID = :P_BATCH_ID;

            -- 2. COPY INTO with lineage columns
            v_sql := 'COPY INTO ' || v_fqn ||
                     ' FROM (SELECT ' || v_select ||
                     '''' || P_BATCH_ID || ''', METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()' ||
                     ' FROM @HC_MIGRATION_DB.RAW.STG_S3_LANDING/incoming/' || P_BATCH_ID || '/)' ||
                     ' FILES = (''' || v_file || ''')' ||
                     ' FILE_FORMAT = (FORMAT_NAME = ''HC_MIGRATION_DB.RAW.FF_PIPE_SOURCE'')' ||
                     ' ON_ERROR = ''CONTINUE''' ||
                     ' FORCE = TRUE';
            EXECUTE IMMEDIATE :v_sql;

            -- 3. count what landed
            SELECT COUNT(*) INTO :v_rows FROM IDENTIFIER(:v_fqn) WHERE BATCH_ID = :P_BATCH_ID;

            IF (v_rows > 0) THEN
                v_status := 'SUCCESS';
                v_msg    := 'Loaded ' || v_file;
            ELSE
                v_status := 'WARNING';
                v_msg    := 'No rows loaded from ' || v_file || ' - check the file exists in s3://<your-landing-bucket>/incoming/' || P_BATCH_ID || '/';
            END IF;
        EXCEPTION
            WHEN OTHER THEN
                v_rows   := 0;
                v_status := 'ERROR';
                v_msg    := 'Load of ' || v_file || ' failed: ' || SQLERRM;
                v_errors := v_errors + 1;
        END;

        INSERT INTO HC_MIGRATION_DB.AUDIT.MIG_LOAD_LOG (BATCH_ID, STEP_NAME, OBJECT_NAME, ROW_COUNT, STATUS, MESSAGE)
        VALUES (:P_BATCH_ID, 'LOAD_RAW', :v_tbl, :v_rows, :v_status, :v_msg);

        v_summary := v_summary || v_tbl || '=' || v_rows || ' (' || v_status || ') ';
    END FOR;

    IF (v_errors > 0) THEN
        UPDATE HC_MIGRATION_DB.AUDIT.MIG_BATCH_CONTROL SET STATUS = 'FAILED', ENDED_AT = CURRENT_TIMESTAMP()
        WHERE BATCH_ID = :P_BATCH_ID;
        RETURN 'LOAD_RAW finished with ' || v_errors || ' error(s): ' || v_summary;
    END IF;

    RETURN 'LOAD_RAW complete: ' || v_summary;
END;
$$;
