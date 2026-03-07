SELECT
    cast(bene_mbi_id as {{ dbt.type_string() }}) as person_id
  , cast(bene_mbi_id as {{ dbt.type_string() }}) as patient_id
  , cast({{ format_yyyymm('enroll_month') }} as {{ dbt.type_string() }}) as year_month
  , cast('Medicare' as {{ dbt.type_string() }}) as payer
  , cast(null as {{ dbt.type_string() }}) as {{ quote_column('plan') }}
  , cast('CMS ALR' as {{ dbt.type_string() }}) as data_source
  , cast(top_npi as {{ dbt.type_string() }}) as payer_attributed_provider
  , cast(top_tin as {{ dbt.type_string() }}) as payer_attributed_provider_practice
  , cast(null as {{ dbt.type_string() }}) as payer_attributed_provider_organization
  , cast(null as {{ dbt.type_string() }}) as payer_attributed_provider_lob
  , cast(null as {{ dbt.type_string() }}) as custom_attributed_provider
  , cast(null as {{ dbt.type_string() }}) as custom_attributed_provider_practice
  , cast(null as {{ dbt.type_string() }}) as custom_attributed_provider_organization
  , cast(null as {{ dbt.type_string() }}) as custom_attributed_provider_lob
  , cast(null as {{ dbt.type_string() }}) as tuva_last_run
FROM {{ ref('aalr_history_filtered') }}
WHERE enroll_flag > 0
