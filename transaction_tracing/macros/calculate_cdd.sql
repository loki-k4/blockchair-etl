{% macro calculate_cdd(lifespan_secs, value_sats) %}
    ({{ lifespan_secs }} / 86400.0) * ({{ value_sats }} / 100000000.0)  -- Days * BTC
{% endmacro %}