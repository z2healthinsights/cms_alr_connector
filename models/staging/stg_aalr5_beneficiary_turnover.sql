SELECT
  cast(bene_mbi_id as {{ dbt.type_string() }} ) as bene_mbi_id,
	cast(bene_hic_num as {{ dbt.type_string() }} ) as bene_hic_num,
	cast(bene_1st_name as {{ dbt.type_string() }} ) as bene_1st_name,
	cast(bene_last_name as {{ dbt.type_string() }} ) as bene_last_name,
	cast(bene_sex_cd as {{ dbt.type_string() }} ) as bene_sex_cd,
	{{ try_to_cast_date('bene_brth_dt', "MM/DD/YYYY") }} as bene_brth_dt,
	{{ try_to_cast_date('bene_death_dt', "MM/DD/YYYY") }} as bene_death_dt,
	cast(plur_r05 as integer) as plur_r05,
	cast(ab_r01 as integer) as ab_r01,
	cast(hmo_r03 as integer) as hmo_r03,
	cast(no_us_r02 as integer) as no_us_r02,
	cast(mdm_r04 as integer) as mdm_r04,
	cast(nofnd_r06 as integer) as nofnd_r06,
	cast(directory_name as {{ dbt.type_string() }} ) as directory_name,
	cast(file_name as {{ dbt.type_string() }} ) as file_name,
	{{ extract_file_metadata() }}
FROM {{ source('cms_ssp_reports', 'aalr5_beneficiary_turnover')}}