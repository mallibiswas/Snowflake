{% macro create_null_string_to_null_function() %}

create or replace function null_string_to_null(text string)
  returns string
  as
  $$

     iff(trim(text)='',NULL,text)
 
  $$
  ;

{% endmacro %}
