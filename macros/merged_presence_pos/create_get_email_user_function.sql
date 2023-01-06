{% macro create_get_email_user_function() %}

create or replace function get_email_user(contact string)
  returns string
  as
  $$
  
  iff(contact like '%@%' -- minimum email validity check
          , regexp_replace(split_part(contact, '@', 1), '[^a-zA-Z0-9@_]') 
          , NULL)
    
  $$
  ;

{% endmacro %}
