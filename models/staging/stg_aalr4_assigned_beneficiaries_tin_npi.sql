SELECT
  cast(bene_mbi_id as {{ dbt.type_string() }} )  as bene_mbi_id,
	cast(bene_hic_num as {{ dbt.type_string() }} )  as bene_hic_num,
	cast(bene_1st_name as {{ dbt.type_string() }} )  as bene_1st_name,
	cast(bene_last_name as {{ dbt.type_string() }} )  as bene_last_name,
	cast(bene_sex_cd as {{ dbt.type_string() }} )  as bene_sex_cd,
	{{ try_to_cast_date('bene_brth_dt', "MM/DD/YYYY") }} as bene_brth_dt,
	{{ try_to_cast_date('bene_death_dt', "MM/DD/YYYY") }} as bene_death_dt,
	cast(master_id as {{ dbt.type_string() }} ) as master_id,
	cast(npi_used as {{ dbt.type_string() }} ) as npi_used,
	cast(pcs_count as integer) as pcs_count,
	cast(directory_name as {{ dbt.type_string() }} ) as directory_name,
	cast(file_name as {{ dbt.type_string() }} ) as file_name,
	{{ extract_file_metadata() }}
FROM {{ source('cms_ssp_reports', 'aalr4_assigned_beneficiaries_tin_npi')}}