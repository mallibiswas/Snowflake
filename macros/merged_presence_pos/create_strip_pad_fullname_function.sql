{% macro create_strip_pad_fullname_function() %}

create or replace function strip_pad_fullname(fullname string, width int, c string)
  returns string
  as
  $$

    case when clean_name(fullname) is NULL or trim(clean_name(fullname)) = '' then repeat(c, width)
         when len(regexp_replace(fullname, '[^a-zA-Z]')) < width then lower(lpad(regexp_replace(fullname, '[^a-zA-Z]'), width, c))
         else lower(regexp_replace(fullname, '[^a-zA-Z]'))
    end
 
  $$
  ;

{% endmacro %}
