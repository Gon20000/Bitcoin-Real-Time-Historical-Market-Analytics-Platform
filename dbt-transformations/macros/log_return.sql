{% macro calculate_log_return(current_price, previous_price) %}
    case
        when {{ previous_price }} is null or {{ previous_price }} <= 0 then null
        when {{ current_price }} is null or {{ current_price }} <= 0 then null
        else cast(ln(cast({{ current_price }} as double) / cast({{ previous_price }} as double)) as decimal(18, 8))
    end
{% endmacro %}
