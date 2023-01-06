{% macro create_get_first_initial_function() %}

create or replace function get_first_initial(fullname string)
  returns string
  as
  $$

       substring(clean_name(fullname),1,1)
 
  $$
  ;

{% endmacro %}
