{% macro create_get_middle_initial_function() %}

create or replace function get_middle_initial(fullname string)
  returns string
  as
  $$

       case 
            when array_size(split(clean_name(fullname), ' ')) = 3 and length(split_part(clean_name(fullname), ' ', 2)) = 1 then lower(split_part(clean_name(fullname), ' ', 2)) 
            when array_size(split(clean_name(fullname), ' ')) = 3 and length(split_part(clean_name(fullname), ' ', 2)) > 1 then substring(lower(split_part(clean_name(fullname), ' ', 2)),1,1)
            else null 
            end
 
  $$
  ;

{% endmacro %}
