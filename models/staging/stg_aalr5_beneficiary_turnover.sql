SELECT
  BENE_MBI_ID,
	BENE_HIC_NUM,
	BENE_1ST_NAME,
	BENE_LAST_NAME,
	BENE_SEX_CD,
	BENE_BRTH_DT,
	BENE_DEATH_DT,
	PLUR_R05,
	AB_R01,
	HMO_R03,
	NO_US_R02,
	MDM_R04,
	NOFND_R06,
	FILE_PATH,
	DIRECTORY_NAME,
	FILE_NAME,
	{{ extract_file_metadata() }}
FROM {{ source('cms_ssp_reports', 'aalr5_beneficiary_turnover')}}