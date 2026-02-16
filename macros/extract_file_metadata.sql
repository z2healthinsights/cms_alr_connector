{% macro extract_file_metadata() %}
split_part(FILE_NAME, '.', 4) as FILE_TYPE,
    CASE WHEN split_part(FILE_NAME, '.', 5) LIKE 'D%'
        THEN concat('Y20', substring(split_part(FILE_NAME, '.', 5),2,2))
        ELSE split_part(FILE_NAME, '.', 5)
        END AS FILE_PERIOD,
    CASE WHEN split_part(FILE_NAME, '.', 5) LIKE 'D%'
      THEN concat('20', substring(split_part(FILE_NAME, '.', 5),2,2))
      ELSE concat('20', substring(split_part(FILE_NAME, '.', 6),2,2)) END as PERFORMANCE_YEAR,
    CASE WHEN split_part(FILE_NAME, '.', 5) LIKE 'D%'
          THEN split_part(FILE_NAME, '.', 6)
          ELSE split_part(FILE_NAME, '.', 7) END as ITERATION
{% endmacro %}