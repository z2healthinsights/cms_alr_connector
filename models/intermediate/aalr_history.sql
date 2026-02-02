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
)

SELECT
  row_number() OVER(PARTITION BY BENE_MBI_ID, enroll_month ORDER BY priority, performance_year DESC, ITERATION DESC) AS row_number,
  *
FROM add_calculated_month