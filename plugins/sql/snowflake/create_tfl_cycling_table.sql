CREATE OR REPLACE TABLE rented_cycles_usage
    USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    WITHIN GROUP (ORDER BY ORDER_ID)
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION=>'@tfl_cycling_s3_stage',
          FILE_FORMAT=>'tfl_cycling_parquet_format'
        )
      ));

-- Copy the data from s3 stage into native table
COPY INTO rented_cycles_usage
 FROM @tfl_cycling_s3_stage
 ON_ERROR='SKIP_FILE'
 MATCH_BY_COLUMN_NAME='CASE_SENSITIVE';