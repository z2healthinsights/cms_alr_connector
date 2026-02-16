SELECT
  BENE_MBI_ID,
	BENE_HIC_NUM,
	BENE_1ST_NAME,
	BENE_LAST_NAME,
	BENE_SEX_CD,
	BENE_BRTH_DT,
	BENE_DEATH_DT,
	VA_SELECTION_ONLY,
	DIRECTORY_NAME,
	FILE_NAME,
	{{ extract_file_metadata() }}
FROM {{ source('cms_ssp_reports', 'aalr6_beneficiaries_assignable_or_voluntary')}}