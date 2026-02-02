SELECT
  BENE_MBI_ID,
	BENE_HIC_NUM,
	BENE_1ST_NAME,
	BENE_LAST_NAME,
	BENE_SEX_CD,
	BENE_BRTH_DT,
	BENE_DEATH_DT,
	VA_SELECTION_ONLY,
	FILE_PATH,
	DIRECTORY_NAME,
	FILE_NAME,
	string_split(FILE_NAME, '.')[4] as FILE_TYPE,
    CASE WHEN (string_split(FILE_NAME, '.'))[5] LIKE 'D%'
        THEN concat('Y20', substring(string_split(FILE_NAME, '.')[5],2,2))
        ELSE (string_split(FILE_NAME, '.'))[5]
        END AS FILE_PERIOD,
    CASE WHEN (string_split(FILE_NAME, '.'))[5] LIKE 'D%'
      THEN concat('20', substring(string_split(FILE_NAME, '.')[5],2,2))
      ELSE concat('20', substring(string_split(FILE_NAME, '.')[6],2,2)) END as PERFORMANCE_YEAR,
    CASE WHEN (string_split(FILE_NAME, '.'))[5] LIKE 'D%'
          THEN (string_split(FILE_NAME, '.'))[6] 
          ELSE (string_split(FILE_NAME, '.'))[7] END as ITERATION
FROM {{ source('cms_ssp_reports', 'aalr6_beneficiaries_assignable_or_voluntary')}}