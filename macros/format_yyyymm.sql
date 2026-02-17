{% macro format_yyyymm(date_col) %}
concat(substring(cast({{ date_col }} as string), 1, 4), substring(cast({{ date_col }} as string), 6, 2))
{% endmacro %}