SELECT
  BENE_MBI_ID,
	BENE_HIC_NUM,
	BENE_1ST_NAME,
	BENE_LAST_NAME,
	BENE_SEX_CD,
	BENE_BRTH_DT,
	BENE_DEATH_DT,
	MASTER_ID,
	B_EM_LINE_CNT_T,
	FILE_PATH,
	DIRECTORY_NAME,
	FILE_NAME,
	{{ extract_file_metadata() }}
FROM {{ source('cms_ssp_reports', 'aalr2_assigned_beneficiaries_tin')}}