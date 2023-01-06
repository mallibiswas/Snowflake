{% macro create_get_initials_function() %}

create or replace function get_initials(fullname string)
  returns string
  as
  $$

       get_first_initial(fullname)||get_last_initial(fullname)
 
  $$
  ;

{% endmacro %}
