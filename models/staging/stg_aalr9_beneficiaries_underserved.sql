SELECT
  BENE_MBI_ID,
	ADI_NATRANK,
	BENE_LIS_STATUS,
	BENE_DUAL_STATUS,
	BENE_PSNYRS_LIS_DUAL,
	BENE_PSNYRS,
	FILE_PATH,
	DIRECTORY_NAME,
	FILE_NAME,
	{{ extract_file_metadata() }}
FROM {{ source('cms_ssp_reports', 'aalr9_beneficiaries_underserved')}}