{% macro convert_to_btc(satoshis) %}
    {{ satoshis }} / 100000000.0  -- Convert satoshis to BTC
{% endmacro %}