{% macro create_strip_pad_function() %}

create or replace function strip_pad(text string, width int, c string)
  returns string
  as
  $$
    
    case when len(regexp_replace(text, '[^a-zA-Z0-9]')) = 0 then repeat(c, width)
         when len(regexp_replace(text, '[^a-zA-Z0-9]')) < width then lpad(regexp_replace(text, '[^a-zA-Z0-9]'), width, c)
         else regexp_replace(text, '[^a-zA-Z0-9]')
    end
    
  $$
  ;

{% endmacro %}
