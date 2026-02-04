{% macro extract_file_metadata() %}
(string_split(FILE_NAME, '.'))[4] as FILE_TYPE,
    CASE WHEN (string_split(FILE_NAME, '.'))[5] LIKE 'D%'
        THEN concat('Y20', substring((string_split(FILE_NAME, '.'))[5],2,2))
        ELSE (string_split(FILE_NAME, '.'))[5]
        END AS FILE_PERIOD,
    CASE WHEN (string_split(FILE_NAME, '.'))[5] LIKE 'D%'
      THEN concat('20', substring((string_split(FILE_NAME, '.'))[5],2,2))
      ELSE concat('20', substring((string_split(FILE_NAME, '.'))[6],2,2)) END as PERFORMANCE_YEAR,
    CASE WHEN (string_split(FILE_NAME, '.'))[5] LIKE 'D%'
          THEN (string_split(FILE_NAME, '.'))[6]
          ELSE (string_split(FILE_NAME, '.'))[7] END as ITERATION
{% endmacro %}