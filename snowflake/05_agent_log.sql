/*==============================================================================
  Agent audit trail: every run and every decision the pipeline agent made.
    AGENT_RUN           one row per run: batch, mode, model, final status, summary
    AGENT_DECISION_LOG  one row per step: tool, the agent's reason, guard verdict,
                        human approval, and the (short) tool result
==============================================================================*/
USE ROLE MIG_DEVELOPER;
USE WAREHOUSE MIG_WH;
USE DATABASE HC_MIGRATION_DB;

CREATE TABLE IF NOT EXISTS AUDIT.AGENT_RUN (
    RUN_ID         VARCHAR(100)  NOT NULL,
    BATCH_ID       VARCHAR(100)  NOT NULL,
    APPROVAL_MODE  VARCHAR(10)   NOT NULL,     -- auto / human
    MODEL          VARCHAR(50),
    STARTED_AT     TIMESTAMP_LTZ NOT NULL,
    ENDED_AT       TIMESTAMP_LTZ,
    FINAL_STATUS   VARCHAR(20),                -- LOADED / QUARANTINED / NEEDS_HUMAN
    SUMMARY        VARCHAR(4000),              -- the agent's plain-English summary
    STEPS          NUMBER
);

CREATE TABLE IF NOT EXISTS AUDIT.AGENT_DECISION_LOG (
    RUN_ID         VARCHAR(100)  NOT NULL,
    BATCH_ID       VARCHAR(100)  NOT NULL,
    STEP_NO        NUMBER        NOT NULL,
    TOOL           VARCHAR(50)   NOT NULL,
    REASON         VARCHAR(2000),              -- why the agent chose this tool
    ARGS           VARIANT,
    VERDICT        VARCHAR(10),                -- ALLOWED / BLOCKED (by the guard)
    APPROVAL       VARCHAR(20),                -- human mode: APPROVED / REJECTED / TIMED_OUT
    RESULT_OK      BOOLEAN,
    RESULT         VARIANT,
    LOGGED_AT      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE VIEW AUDIT.V_AGENT_RUNS AS
SELECT r.RUN_ID, r.BATCH_ID, r.APPROVAL_MODE, r.MODEL, r.STARTED_AT,
       DATEDIFF('second', r.STARTED_AT, r.ENDED_AT) AS SECONDS, r.FINAL_STATUS, r.STEPS, r.SUMMARY,
       LISTAGG(d.TOOL, ' > ') WITHIN GROUP (ORDER BY d.STEP_NO) AS PATH
FROM AUDIT.AGENT_RUN r
LEFT JOIN AUDIT.AGENT_DECISION_LOG d ON d.RUN_ID = r.RUN_ID AND d.VERDICT = 'ALLOWED'
GROUP BY ALL;
