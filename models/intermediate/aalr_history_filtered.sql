-- Find the latest file (by minimum priority) that was received for each enroll_month for each performance_year
WITH latest_priority AS (
  SELECT
    performance_year,
    enroll_month,
    MIN(priority) as min_priority
  FROM {{ ref('aalr_history')}}
  GROUP BY
    performance_year,
    enroll_month
)

-- Get latest priority of record based on row_number, and only keep records that were received on latest file for that report period
SELECT
  ap.row_number,
  ap.enroll_month,
  ap.month_number,
  ap.enroll_flag,
  ap.bene_mbi_id,
  ap.bene_hic_num,
  ap.bene_1st_name,
  ap.bene_last_name,
  ap.bene_sex_cd,
  ap.bene_brth_dt,
  ap.bene_death_dt,
  ap.geo_ssa_cnty_cd_name,
  ap.geo_ssa_state_name,
  ap.state_county_cd,
  ap.in_va_max,
  ap.va_tin,
  ap.va_npi,
  ap.cba_flag,
  ap.assignment_type,
  ap.assigned_before,
  ap.asg_status,
  ap.partd_months,
  ap.hcc_version,
  {% for i in range(1, 121) %}
  ap.hcc_col_{{ i }},
  {% endfor %}
  {% for i in range(1, 13) %}
  ap.bene_rsk_r_scre_{{ '%02d' % i }},
  {% endfor %}
  ap.esrd_score,
  ap.dis_score,
  ap.agdu_score,
  ap.agnd_score,
  ap.dem_esrd_score,
  ap.dem_dis_score,
  ap.dem_agdu_score,
  ap.dem_agnd_score,
  ap.new_enrollee,
  ap.lti_status,
  ap.bene_race_cd,
  ap.bene_psnyrs_dual,
  ap.directory_name,
  ap.file_name,
  ap.FILE_TYPE,
  ap.FILE_PERIOD,
  ap.PERFORMANCE_YEAR,
  ap.ITERATION,
  ap.report_year,
  ap.period_start_date,
  ap.period_end_date,
  ap.priority,
  ap.alr_file_type,
  ap.TOP_TIN,
  ap.TIN_EM_COUNT,
  ap.TOP_NPI,
  ap.NPI_EM_COUNT,
  ap.VA_SELECTION_ONLY,
  ap.ADI_NATRANK,
  ap.BENE_LIS_STATUS,
  ap.BENE_DUAL_STATUS,
  ap.BENE_PSNYRS_LIS_DUAL,
  ap.BENE_PSNYRS,
  ap.plur_r05,
  ap.ab_r01,
  ap.hmo_r03,
  ap.no_us_r02,
  ap.mdm_r04,
  ap.nofnd_r06
FROM {{ ref('aalr_history')}} as ap
LEFT JOIN latest_priority as lp ON ap.enroll_month = lp.enroll_month AND ap.performance_year = lp.performance_year
WHERE row_number = 1
  AND ap.priority = lp.min_priority
