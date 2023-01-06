{% macro create_education_email_function() %}

create or replace function education_email(email string)
  returns boolean
  as
  $$

        email like '%.edu'
        or email like '%utoronto.ca'
        or email like '%robeson.k12.nc.us%'
        or email like '%hec.ca'
 
  $$
  ;

{% endmacro %}
