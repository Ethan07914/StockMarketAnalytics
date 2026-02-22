{% test is_negative(model, column_name) %}
-- Checks if a column contains negative values which should not be possible given the context of the column

select
       *
from
       {{ model }}
where
       {{ column_name }} < 0

{% endtest %}
