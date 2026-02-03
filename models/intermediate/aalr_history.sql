-- Create scaffold of 12 month numbers to join against to enrollflags
WITH scaffold_months AS (
    {% for month in range(1, 13) %}
      SELECT {{ month }} AS month_number
      {% if not loop.last %}
        UNION ALL
      {% endif %}
    {% endfor %}
),

-- Cross join scaffoled months to each enrollment record, set enroll_flag if
-- enrollflag is set for the given month
-- Example: 
-- enrollflag1 and month_number = 1 then enrollflag1 value, otherwise NULL
-- enrollflag2 and month_number = 2 then enrollflag2 value, otherwise NULL
aalr_exploded_by_enrollflag AS (
  SELECT
    sm.month_number,
    CASE sm.month_number
        {% for month in range(1, 13) %}
        WHEN {{ month }} THEN b.enrollflag{{ month }}
        {% endfor %}
    END AS enroll_flag,
    b.*
  FROM {{ ref('stg_aalr1_assigned_beneficiaries')}} b
  CROSS JOIN scaffold_months sm
),

add_calculated_month AS (
  SELECT 
    CAST(date_add(mfp.period_start_date, INTERVAL (aalr.month_number - 1) MONTH) AS date) as enroll_month, -- @TODO - Refactor to macro
    aalr.*,
    mfp.*
  FROM aalr_exploded_by_enrollflag as aalr
  LEFT JOIN {{ ref('mssp_file_parameters') }} as mfp ON aalr.PERFORMANCE_YEAR = mfp.PERFORMANCE_YEAR AND aalr.file_period = mfp.file_period
),

top_tin AS (
  SELECT
    file_period,
    performance_year,
    ITERATION,
    BENE_MBI_ID,
    MASTER_ID,
    B_EM_LINE_CNT_T
  FROM {{ ref('stg_aalr2_assigned_beneficiaries_tin') }}
  -- @TODO: Refactor qualify statement
  QUALIFY ROW_NUMBER() OVER(PARTITION BY BENE_MBI_ID, file_period, performance_year, ITERATION ORDER BY B_EM_LINE_CNT_T DESC) = 1
),

top_npi AS (
  SELECT
    BENE_MBI_ID,
    file_period,
    performance_year,
    ITERATION,
    MASTER_ID,
    NPI_USED,
    PCS_COUNT
  FROM {{ ref('stg_aalr4_assigned_beneficiaries_tin_npi') }}
  -- @TODO: Refactor qualify statement
  QUALIFY ROW_NUMBER() OVER(PARTITION BY BENE_MBI_ID, file_period, performance_year, ITERATION, MASTER_ID ORDER BY PCS_COUNT DESC) = 1
),

beneficiary_turnover AS (
  SELECT
    row_number() OVER(PARTITION BY bt.BENE_MBI_ID, bt.performance_year ORDER BY mfp.priority, bt.ITERATION DESC) AS row_number,
    bt.*
  FROM {{ ref('stg_aalr5_beneficiary_turnover') }} as bt
  LEFT JOIN {{ ref('mssp_file_parameters') }} as mfp ON bt.PERFORMANCE_YEAR = mfp.PERFORMANCE_YEAR AND bt.file_period = mfp.file_period
),

latest_beneficiary_turnover AS (
  SELECT
    *
  FROM beneficiary_turnover
  WHERE row_number = 1
)

SELECT
  row_number() OVER(PARTITION BY acm.BENE_MBI_ID, acm.enroll_month ORDER BY acm.priority, acm.performance_year DESC, acm.ITERATION DESC) AS row_number,
  acm.*,
  tt.MASTER_ID as TOP_TIN,
  tt.B_EM_LINE_CNT_T as TIN_EM_COUNT,
  tn.NPI_USED as TOP_NPI,
  tn.PCS_COUNT as NPI_EM_COUNT,
  baov.VA_SELECTION_ONLY,
  bu.ADI_NATRANK,
  bu.BENE_LIS_STATUS,
  bu.BENE_DUAL_STATUS,
  bu.BENE_PSNYRS_LIS_DUAL,
  BENE_PSNYRS,
  lbt.plur_r05,
  lbt.ab_r01,
  lbt.hmo_r03,
  lbt.no_us_r02,
  lbt.mdm_r04,
  lbt.nofnd_r06
FROM add_calculated_month as acm
LEFT JOIN top_tin as tt
  ON acm.BENE_MBI_ID = tt.BENE_MBI_ID
  AND acm.FILE_PERIOD = tt.FILE_PERIOD
  AND acm.PERFORMANCE_YEAR = tt.PERFORMANCE_YEAR
  -- AND acm.ITERATION = tt.ITERATION -- Normalize iterations to not include file types, then add back in
LEFT JOIN top_npi AS tn
  ON acm.BENE_MBI_ID = tn.BENE_MBI_ID
  -- Only grap NPIs from the top TIN (it's possible an individual NPI NOT assigned to the top TIN has the most visits)
  AND tt.MASTER_ID = tn.MASTER_ID 
  AND acm.FILE_PERIOD = tn.FILE_PERIOD
  AND acm.PERFORMANCE_YEAR = tn.PERFORMANCE_YEAR
  -- AND acm.ITERATION = tn.ITERATION -- Normalize iterations to not include file types, then add back in
LEFT JOIN {{ ref('stg_aalr6_beneficiaries_assignable_or_voluntary') }} as baov
  ON acm.BENE_MBI_ID = baov.BENE_MBI_ID
  AND acm.FILE_PERIOD = baov.FILE_PERIOD
  AND acm.PERFORMANCE_YEAR = baov.PERFORMANCE_YEAR
  -- AND acm.ITERATION = baov.ITERATION -- Normalize iterations to not include file types, then add back in
LEFT JOIN {{ ref('stg_aalr9_beneficiaries_underserved')}} as bu
  ON acm.BENE_MBI_ID = bu.BENE_MBI_ID
  AND acm.FILE_PERIOD = bu.FILE_PERIOD
  AND acm.PERFORMANCE_YEAR = bu.PERFORMANCE_YEAR
  -- AND acm.ITERATION = bu.ITERATION -- Normalize iterations to not include file types, then add back in
LEFT JOIN latest_beneficiary_turnover as lbt
  ON acm.BENE_MBI_ID = lbt.BENE_MBI_ID AND acm.performance_year = lbt.performance_year