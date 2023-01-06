{% macro create_get_email_domain_function() %}

create or replace function get_email_domain(contact string)
  returns string
  as
  $$
  
  iff(contact like '%@%' -- minimum email validity check
          , regexp_replace(split_part(contact, '@', 2), '[^a-zA-Z0-9_.-]') 
          , NULL)
    
  $$
  ;

{% endmacro %}
