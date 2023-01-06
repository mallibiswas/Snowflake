{% macro create_zenreach_email_function() %}

create or replace function zenreach_email(email string)
  returns boolean
  as
  $$

        email like '%zenreach.com'
        or email like '%greenfieldlabs.cca'
 
  $$
  ;

{% endmacro %}
