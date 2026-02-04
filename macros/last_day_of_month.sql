{% macro last_day_of_month(date_col) %}
{{ dbt.dateadd('day', -1, dbt.dateadd('month', 1, dbt.date_trunc('month', date_col))) }}
{% endmacro %}