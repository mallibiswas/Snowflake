{% macro create_strip_pad_email_user_function() %}

create or replace function strip_pad_email_user(email string, width int, c string)
  returns string
  as
  $$

    case when not contains(email, '@') or email is null then repeat(c, width)
         when len(regexp_replace(split_part(email, '@', 1), '[^a-zA-Z]')) < width then lpad(regexp_replace(split_part(email, '@', 1), '[^a-zA-Z]'), width, c)
         else regexp_replace(split_part(email, '@', 1), '[^a-zA-Z]')
    end
 
  $$
  ;

{% endmacro %}
