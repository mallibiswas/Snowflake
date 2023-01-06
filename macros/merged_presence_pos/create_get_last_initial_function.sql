{% macro create_get_last_initial_function() %}

create or replace function get_last_initial(fullname string)
  returns string
  as
  $$

       substring(get_last_name(fullname),1,1)
 
  $$
  ;

{% endmacro %}
