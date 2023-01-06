{% macro create_get_first_name_function() %}

create or replace function get_first_name(fullname string)
  returns string
  as
  $$

       lower(split_part(clean_name(fullname), ' ', 1))
 
  $$
  ;

{% endmacro %}
