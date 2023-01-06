{% macro create_clean_name_function() %}

create or replace function clean_name(fullname string)
  returns string
  as
  $$
  
     null_string_to_null(regexp_replace(regexp_replace(regexp_replace(lower(trim(fullname)), '[^a-zA-Z ]'), '(\\s+)', ' '), '^none$|^null$| jr$| iii$| ii$| sr$| iv$| dc$| md$|^mr |^mrs |^dr |^ms ', ''))

  $$
  ;

{% endmacro %}
