-- Create scaffold of 12 months to join against to enrollflags
WITH months AS (
    {% for month in range(1, 13) %}
      SELECT {{ month }} AS month_number
      {% if not loop.last %}
        UNION ALL
      {% endif %}
    {% endfor %}
),

aalr_exploded_by_enrollflag AS (
  SELECT
    months.month_number,
    CASE months.month_number
        {% for month in range(1, 13) %}
        WHEN {{ month }} THEN b.enrollflag{{ month }}
        {% endfor %}
    END AS enrollflag,
    CASE WHEN file_period = 'Y2022' THEN DATE '2022-01-01'
        WHEN file_period = 'Y2023' THEN DATE '2023-01-01'
        WHEN file_period = 'Y2024' THEN DATE '2024-01-01'
        WHEN file_period = '2025Q2' THEN DATE'2024-07-01' ELSE NULL
        END AS start_date,
    b.*
  FROM {{ ref('stg_aalr1_assigned_beneficiaries')}} b
  CROSS JOIN months
  WHERE file_period IN ('Y2022', 'Y2023', 'Y2024', '2025Q2')
)



SELECT 
  date_add(start_date, INTERVAL (MonthNumber - 1) MONTH) as enroll_month,
  aalr.* 
FROM aalr_exploded_by_enrollflag as aalr
