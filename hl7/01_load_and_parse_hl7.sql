/* =====================================================================
   HL7 v2.5.1 demo: load raw messages, then parse into 6 tables
   Source file : hl7.csv  (MESSAGE_ID, RECEIVED_TIMESTAMP, RAW_MESSAGE)
   Facility    : Front Range Regional Medical Center (FRRMC), fictional
   Messages    : ADT^A01/A02/A03/A04/A08, ORM^O01, ORU^R01
   Output      : PATIENTS, ENCOUNTERS, DIAGNOSES, ORDERS, OBSERVATIONS, PROVIDERS
   All data is synthetic.
   ===================================================================== */

-- Adjust names to taste
CREATE DATABASE IF NOT EXISTS HL7_DEMO;
CREATE SCHEMA IF NOT EXISTS HL7_DEMO.CLINICAL;
USE SCHEMA HL7_DEMO.CLINICAL;

/* ---------------------------------------------------------------------
   1. Land the raw file
   --------------------------------------------------------------------- */
CREATE OR REPLACE FILE FORMAT HL7_CSV_FORMAT
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ESCAPE_UNENCLOSED_FIELD = NONE      -- keep the backslash in MSH-2 (^~\&) intact
  EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE STAGE HL7_STAGE FILE_FORMAT = HL7_CSV_FORMAT;

-- Upload hl7.csv to @HL7_STAGE (Snowsight: Data > Add Data > Load files into a stage),
-- or from SnowSQL:  PUT file:///path/to/hl7.csv @HL7_STAGE AUTO_COMPRESS=TRUE;

CREATE OR REPLACE TABLE HL7_RAW (
  MESSAGE_ID          STRING,
  RECEIVED_TIMESTAMP  TIMESTAMP_NTZ,
  RAW_MESSAGE         STRING          -- full pipe-delimited message, one segment per line
);

COPY INTO HL7_RAW
  FROM @HL7_STAGE
  PATTERN = '.*hl7[.]csv.*'
  FILE_FORMAT = (FORMAT_NAME = HL7_CSV_FORMAT);

/* ---------------------------------------------------------------------
   2. Explode messages into segments
      FIELDS[n] = HL7 field n for every segment except MSH
      (for MSH, FIELDS[n] = MSH-(n+1) because MSH-1 is the separator itself)
      Segments may be separated by \r (HL7 standard), \r\n or \n.
   --------------------------------------------------------------------- */
CREATE OR REPLACE VIEW HL7_SEGMENTS AS
WITH seg AS (
  SELECT
    r.MESSAGE_ID,
    s.INDEX + 1                         AS SEGMENT_SEQ,
    s.VALUE::STRING                     AS SEGMENT_TEXT,
    SPLIT(s.VALUE::STRING, '|')         AS F
  FROM HL7_RAW r,
       LATERAL FLATTEN(INPUT => SPLIT(REGEXP_REPLACE(r.RAW_MESSAGE, '\r\n|\r', '\n'), '\n')) s
  WHERE TRIM(s.VALUE::STRING) <> ''
),
msh AS (
  SELECT
    MESSAGE_ID,
    SPLIT_PART(F[8]::STRING, '^', 1)                          AS MESSAGE_TYPE,     -- MSH-9.1
    SPLIT_PART(F[8]::STRING, '^', 2)                          AS TRIGGER_EVENT,    -- MSH-9.2
    TRY_TO_TIMESTAMP_NTZ(F[6]::STRING, 'YYYYMMDDHH24MISS')    AS MESSAGE_DATETIME, -- MSH-7
    F[3]::STRING                                              AS SENDING_FACILITY, -- MSH-4
    F[2]::STRING                                              AS SENDING_APPLICATION -- MSH-3
  FROM seg
  WHERE F[0]::STRING = 'MSH'
)
SELECT
  s.MESSAGE_ID,
  m.MESSAGE_TYPE,
  m.TRIGGER_EVENT,
  m.MESSAGE_DATETIME,
  m.SENDING_FACILITY,
  m.SENDING_APPLICATION,
  s.SEGMENT_SEQ,
  s.F[0]::STRING AS SEGMENT_TYPE,
  s.F            AS FIELDS,
  s.SEGMENT_TEXT
FROM seg s
JOIN msh m ON m.MESSAGE_ID = s.MESSAGE_ID;

-- Patient (PID-3) and visit (PV1-19) for every message, used to link child segments
CREATE OR REPLACE VIEW HL7_MESSAGE_CONTEXT AS
SELECT
  MESSAGE_ID,
  MAX(IFF(SEGMENT_TYPE = 'PID', SPLIT_PART(FIELDS[3]::STRING, '^', 1), NULL)) AS MRN,
  MAX(IFF(SEGMENT_TYPE = 'PV1', NULLIF(FIELDS[19]::STRING, ''), NULL))        AS VISIT_NUMBER
FROM HL7_SEGMENTS
GROUP BY MESSAGE_ID;

/* ---------------------------------------------------------------------
   3. PATIENTS  (PID, latest message per MRN)
   --------------------------------------------------------------------- */
CREATE OR REPLACE TABLE PATIENTS AS
SELECT
  SPLIT_PART(FIELDS[3]::STRING, '^', 1)                  AS MRN,
  NULLIF(SPLIT_PART(FIELDS[5]::STRING, '^', 1), '')      AS LAST_NAME,
  NULLIF(SPLIT_PART(FIELDS[5]::STRING, '^', 2), '')      AS FIRST_NAME,
  NULLIF(SPLIT_PART(FIELDS[5]::STRING, '^', 3), '')      AS MIDDLE_NAME,
  TRY_TO_DATE(FIELDS[7]::STRING, 'YYYYMMDD')             AS DATE_OF_BIRTH,
  NULLIF(FIELDS[8]::STRING, '')                          AS SEX,
  NULLIF(SPLIT_PART(FIELDS[10]::STRING, '^', 2), '')     AS RACE,
  NULLIF(SPLIT_PART(FIELDS[11]::STRING, '^', 1), '')     AS ADDRESS_LINE1,
  NULLIF(SPLIT_PART(FIELDS[11]::STRING, '^', 3), '')     AS CITY,
  NULLIF(SPLIT_PART(FIELDS[11]::STRING, '^', 4), '')     AS STATE,
  NULLIF(SPLIT_PART(FIELDS[11]::STRING, '^', 5), '')     AS ZIP,
  NULLIF(SPLIT_PART(FIELDS[13]::STRING, '^', 1), '')     AS PHONE,
  NULLIF(FIELDS[16]::STRING, '')                         AS MARITAL_STATUS,
  MESSAGE_DATETIME                                       AS LAST_UPDATED
FROM HL7_SEGMENTS
WHERE SEGMENT_TYPE = 'PID'
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY SPLIT_PART(FIELDS[3]::STRING, '^', 1)
  ORDER BY MESSAGE_DATETIME DESC, MESSAGE_ID DESC) = 1;

/* ---------------------------------------------------------------------
   4. ENCOUNTERS  (PV1 from ADT messages, one row per PV1-19 visit)
      Location/class come from the most recent ADT event (captures A02 transfers)
   --------------------------------------------------------------------- */
CREATE OR REPLACE TABLE ENCOUNTERS AS
WITH pv1 AS (
  SELECT
    s.MESSAGE_ID,
    s.TRIGGER_EVENT,
    s.MESSAGE_DATETIME,
    c.MRN,
    s.FIELDS[19]::STRING                                         AS VISIT_NUMBER,
    s.FIELDS[2]::STRING                                          AS PATIENT_CLASS,
    NULLIF(s.FIELDS[4]::STRING, '')                              AS ADMISSION_TYPE,
    NULLIF(s.FIELDS[14]::STRING, '')                             AS ADMIT_SOURCE,
    NULLIF(s.FIELDS[10]::STRING, '')                             AS HOSPITAL_SERVICE,
    NULLIF(SPLIT_PART(s.FIELDS[3]::STRING, '^', 1), '')          AS POINT_OF_CARE,
    NULLIF(SPLIT_PART(s.FIELDS[3]::STRING, '^', 2), '')          AS ROOM,
    NULLIF(SPLIT_PART(s.FIELDS[3]::STRING, '^', 3), '')          AS BED,
    NULLIF(SPLIT_PART(s.FIELDS[3]::STRING, '^', 4), '')          AS FACILITY,
    NULLIF(SPLIT_PART(s.FIELDS[7]::STRING, '^', 1), '')          AS ATTENDING_PROVIDER_ID,
    NULLIF(s.FIELDS[36]::STRING, '')                             AS DISCHARGE_DISPOSITION,
    TRY_TO_TIMESTAMP_NTZ(s.FIELDS[44]::STRING, 'YYYYMMDDHH24MISS') AS ADMIT_DT,
    TRY_TO_TIMESTAMP_NTZ(s.FIELDS[45]::STRING, 'YYYYMMDDHH24MISS') AS DISCHARGE_DT
  FROM HL7_SEGMENTS s
  JOIN HL7_MESSAGE_CONTEXT c ON c.MESSAGE_ID = s.MESSAGE_ID
  WHERE s.SEGMENT_TYPE = 'PV1' AND s.MESSAGE_TYPE = 'ADT'
),
agg AS (
  SELECT
    VISIT_NUMBER,
    MAX_BY(MRN, MESSAGE_DATETIME)                    AS MRN,
    MAX_BY(PATIENT_CLASS, MESSAGE_DATETIME)          AS PATIENT_CLASS,
    MAX_BY(ADMISSION_TYPE, MESSAGE_DATETIME)         AS ADMISSION_TYPE,
    MAX_BY(ADMIT_SOURCE, MESSAGE_DATETIME)           AS ADMIT_SOURCE,
    MAX_BY(HOSPITAL_SERVICE, MESSAGE_DATETIME)       AS HOSPITAL_SERVICE,
    MAX_BY(POINT_OF_CARE, MESSAGE_DATETIME)          AS POINT_OF_CARE,
    MAX_BY(ROOM, MESSAGE_DATETIME)                   AS ROOM,
    MAX_BY(BED, MESSAGE_DATETIME)                    AS BED,
    MAX_BY(FACILITY, MESSAGE_DATETIME)               AS FACILITY,
    MAX_BY(ATTENDING_PROVIDER_ID, MESSAGE_DATETIME)  AS ATTENDING_PROVIDER_ID,
    MIN(ADMIT_DT)                                    AS ADMIT_DATETIME,
    MAX(DISCHARGE_DT)                                AS DISCHARGE_DATETIME,
    MAX(DISCHARGE_DISPOSITION)                       AS DISCHARGE_DISPOSITION,
    IFF(COUNT_IF(TRIGGER_EVENT = 'A03') > 0, 'Discharged', 'Active') AS ENCOUNTER_STATUS,
    COUNT(DISTINCT MESSAGE_ID)                       AS ADT_MESSAGE_COUNT
  FROM pv1
  GROUP BY VISIT_NUMBER
)
SELECT
  VISIT_NUMBER,
  MRN,
  PATIENT_CLASS,
  CASE PATIENT_CLASS WHEN 'I' THEN 'Inpatient' WHEN 'O' THEN 'Outpatient' WHEN 'E' THEN 'Emergency' END
                                                   AS PATIENT_CLASS_DESC,
  ADMISSION_TYPE,
  ADMIT_SOURCE,
  HOSPITAL_SERVICE,
  POINT_OF_CARE,
  ROOM,
  BED,
  FACILITY,
  ATTENDING_PROVIDER_ID,
  ADMIT_DATETIME,
  DISCHARGE_DATETIME,
  DISCHARGE_DISPOSITION,
  CASE DISCHARGE_DISPOSITION
    WHEN '01' THEN 'Discharged to home'
    WHEN '03' THEN 'Discharged to skilled nursing facility'
    WHEN '06' THEN 'Discharged to home health care'
    WHEN '07' THEN 'Left against medical advice'
  END                                              AS DISCHARGE_DISPOSITION_DESC,
  DATEDIFF('day', ADMIT_DATETIME::DATE, DISCHARGE_DATETIME::DATE) AS LENGTH_OF_STAY_DAYS,
  ENCOUNTER_STATUS,
  ADT_MESSAGE_COUNT
FROM agg;

/* ---------------------------------------------------------------------
   5. DIAGNOSES  (DG1; A = admitting, W = working, F = final)
   --------------------------------------------------------------------- */
CREATE OR REPLACE TABLE DIAGNOSES AS
SELECT DISTINCT
  c.VISIT_NUMBER,
  c.MRN,
  SPLIT_PART(d.FIELDS[3]::STRING, '^', 1)                         AS DIAGNOSIS_CODE,
  SPLIT_PART(d.FIELDS[3]::STRING, '^', 2)                         AS DIAGNOSIS_DESCRIPTION,
  SPLIT_PART(d.FIELDS[3]::STRING, '^', 3)                         AS CODING_SYSTEM,
  NULLIF(d.FIELDS[6]::STRING, '')                                 AS DIAGNOSIS_TYPE,
  CASE d.FIELDS[6]::STRING WHEN 'A' THEN 'Admitting' WHEN 'W' THEN 'Working' WHEN 'F' THEN 'Final' END
                                                                  AS DIAGNOSIS_TYPE_DESC,
  TRY_TO_TIMESTAMP_NTZ(d.FIELDS[5]::STRING, 'YYYYMMDDHH24MISS')   AS DIAGNOSIS_DATETIME,
  TRY_TO_NUMBER(d.FIELDS[15]::STRING)                             AS DIAGNOSIS_PRIORITY
FROM HL7_SEGMENTS d
JOIN HL7_MESSAGE_CONTEXT c ON c.MESSAGE_ID = d.MESSAGE_ID
WHERE d.SEGMENT_TYPE = 'DG1';

/* ---------------------------------------------------------------------
   6. ORDERS  (ORC + OBR; ORM = placed, ORU = resulted; one order per message)
   --------------------------------------------------------------------- */
CREATE OR REPLACE TABLE ORDERS AS
WITH o AS (
  SELECT
    orc.MESSAGE_ID,
    orc.MESSAGE_TYPE,
    c.MRN,
    c.VISIT_NUMBER,
    orc.FIELDS[2]::STRING                                           AS ORDER_ID,
    NULLIF(orc.FIELDS[3]::STRING, '')                               AS FILLER_ORDER_ID,
    NULLIF(SPLIT_PART(orc.FIELDS[12]::STRING, '^', 1), '')          AS ORDERING_PROVIDER_ID,
    TRY_TO_TIMESTAMP_NTZ(orc.FIELDS[9]::STRING, 'YYYYMMDDHH24MISS') AS TRANSACTION_DT,
    SPLIT_PART(obr.FIELDS[4]::STRING, '^', 1)                       AS ORDER_CODE,
    SPLIT_PART(obr.FIELDS[4]::STRING, '^', 2)                       AS ORDER_NAME,
    SPLIT_PART(obr.FIELDS[4]::STRING, '^', 3)                       AS CODING_SYSTEM,
    NULLIF(obr.FIELDS[24]::STRING, '')                              AS DIAGNOSTIC_SECTION,
    TRY_TO_TIMESTAMP_NTZ(obr.FIELDS[22]::STRING, 'YYYYMMDDHH24MISS') AS RESULT_DT
  FROM HL7_SEGMENTS orc
  JOIN HL7_SEGMENTS obr
    ON obr.MESSAGE_ID = orc.MESSAGE_ID AND obr.SEGMENT_TYPE = 'OBR'
  JOIN HL7_MESSAGE_CONTEXT c ON c.MESSAGE_ID = orc.MESSAGE_ID
  WHERE orc.SEGMENT_TYPE = 'ORC'
)
SELECT
  ORDER_ID,
  ANY_VALUE(FILLER_ORDER_ID)                         AS FILLER_ORDER_ID,
  MAX(VISIT_NUMBER)                                  AS VISIT_NUMBER,
  MAX(MRN)                                           AS MRN,
  ANY_VALUE(ORDER_CODE)                              AS ORDER_CODE,
  ANY_VALUE(ORDER_NAME)                              AS ORDER_NAME,
  ANY_VALUE(CODING_SYSTEM)                           AS CODING_SYSTEM,
  ANY_VALUE(DIAGNOSTIC_SECTION)                      AS DIAGNOSTIC_SECTION,
  ANY_VALUE(ORDERING_PROVIDER_ID)                    AS ORDERING_PROVIDER_ID,
  MIN(IFF(MESSAGE_TYPE = 'ORM', TRANSACTION_DT, NULL)) AS ORDER_DATETIME,
  MAX(IFF(MESSAGE_TYPE = 'ORU', RESULT_DT, NULL))      AS RESULT_DATETIME,
  IFF(COUNT_IF(MESSAGE_TYPE = 'ORU') > 0, 'Resulted', 'Ordered') AS ORDER_STATUS
FROM o
GROUP BY ORDER_ID;

/* ---------------------------------------------------------------------
   7. OBSERVATIONS  (OBX from ORU; NM = numeric, ST = coded text, TX = narrative)
   --------------------------------------------------------------------- */
CREATE OR REPLACE TABLE OBSERVATIONS AS
SELECT
  x.MESSAGE_ID,
  orc.FIELDS[2]::STRING                                         AS ORDER_ID,
  c.VISIT_NUMBER,
  c.MRN,
  TRY_TO_NUMBER(x.FIELDS[1]::STRING)                            AS SET_ID,
  NULLIF(x.FIELDS[2]::STRING, '')                               AS VALUE_TYPE,
  SPLIT_PART(x.FIELDS[3]::STRING, '^', 1)                       AS OBSERVATION_CODE,
  SPLIT_PART(x.FIELDS[3]::STRING, '^', 2)                       AS OBSERVATION_NAME,
  SPLIT_PART(x.FIELDS[3]::STRING, '^', 3)                       AS CODING_SYSTEM,
  NULLIF(x.FIELDS[5]::STRING, '')                               AS OBSERVATION_VALUE,
  IFF(x.FIELDS[2]::STRING = 'NM', TRY_TO_DOUBLE(x.FIELDS[5]::STRING), NULL) AS NUMERIC_VALUE,
  NULLIF(x.FIELDS[6]::STRING, '')                               AS UNITS,
  NULLIF(x.FIELDS[7]::STRING, '')                               AS REFERENCE_RANGE,
  NULLIF(x.FIELDS[8]::STRING, '')                               AS ABNORMAL_FLAG,
  NULLIF(x.FIELDS[11]::STRING, '')                              AS RESULT_STATUS,
  TRY_TO_TIMESTAMP_NTZ(x.FIELDS[14]::STRING, 'YYYYMMDDHH24MISS') AS OBSERVATION_DATETIME
FROM HL7_SEGMENTS x
JOIN HL7_MESSAGE_CONTEXT c ON c.MESSAGE_ID = x.MESSAGE_ID
LEFT JOIN HL7_SEGMENTS orc
  ON orc.MESSAGE_ID = x.MESSAGE_ID AND orc.SEGMENT_TYPE = 'ORC'
WHERE x.SEGMENT_TYPE = 'OBX';

/* ---------------------------------------------------------------------
   8. PROVIDERS  (XCN from PV1-7 attending and ORC-12 ordering provider)
   --------------------------------------------------------------------- */
CREATE OR REPLACE TABLE PROVIDERS AS
WITH refs AS (
  SELECT s.FIELDS[7]::STRING AS XCN, 'ATTENDING' AS ROLE, c.VISIT_NUMBER, NULL::STRING AS ORDER_ID, s.MESSAGE_DATETIME
  FROM HL7_SEGMENTS s
  JOIN HL7_MESSAGE_CONTEXT c ON c.MESSAGE_ID = s.MESSAGE_ID
  WHERE s.SEGMENT_TYPE = 'PV1' AND NULLIF(s.FIELDS[7]::STRING, '') IS NOT NULL
  UNION ALL
  SELECT s.FIELDS[12]::STRING, 'ORDERING', c.VISIT_NUMBER, s.FIELDS[2]::STRING, s.MESSAGE_DATETIME
  FROM HL7_SEGMENTS s
  JOIN HL7_MESSAGE_CONTEXT c ON c.MESSAGE_ID = s.MESSAGE_ID
  WHERE s.SEGMENT_TYPE = 'ORC' AND NULLIF(s.FIELDS[12]::STRING, '') IS NOT NULL
)
SELECT
  SPLIT_PART(XCN, '^', 1)                                   AS PROVIDER_ID,
  ANY_VALUE(NULLIF(SPLIT_PART(XCN, '^', 2), ''))            AS LAST_NAME,
  ANY_VALUE(NULLIF(SPLIT_PART(XCN, '^', 3), ''))            AS FIRST_NAME,
  ANY_VALUE(NULLIF(SPLIT_PART(XCN, '^', 4), ''))            AS MIDDLE_INITIAL,
  ANY_VALUE(NULLIF(SPLIT_PART(XCN, '^', 7), ''))            AS DEGREE,
  COUNT(DISTINCT IFF(ROLE = 'ATTENDING', VISIT_NUMBER, NULL)) AS ATTENDING_ENCOUNTERS,
  COUNT(DISTINCT ORDER_ID)                                  AS ORDERS_PLACED,
  MIN(MESSAGE_DATETIME)                                     AS FIRST_SEEN,
  MAX(MESSAGE_DATETIME)                                     AS LAST_SEEN
FROM refs
GROUP BY SPLIT_PART(XCN, '^', 1);

/* ---------------------------------------------------------------------
   9. Quick checks (expected: 100 / 214 / 768 / 986 / 4,649 / 14)
   --------------------------------------------------------------------- */
SELECT 'PATIENTS' AS TBL, COUNT(*) AS ROWS_LOADED FROM PATIENTS
UNION ALL SELECT 'ENCOUNTERS',   COUNT(*) FROM ENCOUNTERS
UNION ALL SELECT 'DIAGNOSES',    COUNT(*) FROM DIAGNOSES
UNION ALL SELECT 'ORDERS',       COUNT(*) FROM ORDERS
UNION ALL SELECT 'OBSERVATIONS', COUNT(*) FROM OBSERVATIONS
UNION ALL SELECT 'PROVIDERS',    COUNT(*) FROM PROVIDERS;
