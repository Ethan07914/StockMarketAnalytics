{% test fields_equivalent(model, column_1, column_2, column_3) %}
-- Used to check that at least one of the three fields contains a value of one and that not all values contain one

select
       *
from
       {{ model }}
where
       {{ column_1 }} = {{ column_2 }}
       and {{ column_1 }} = {{ column_3 }}

{% endtest %}