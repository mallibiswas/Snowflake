{% macro create_get_last_name_function() %}

-- TODO: this function should automatically lpad, or should parameterize that. Figure out why I put that in here and change it back :P
create or replace function get_last_name(fullname string)
  returns string
  as
  $$

        case 
          when array_size(split(clean_name(fullname), ' ')) = 3 and length(split_part(clean_name(fullname), ' ', 2)) = 2 then lower(split_part(clean_name(fullname), ' ', 2) || split_part(clean_name(fullname), ' ', 3))
            else iff(len(lower(split_part(clean_name(fullname), ' ', -1)))=1, lpad(lower(split_part(clean_name(fullname), ' ', -1)),4, '*'), lower(split_part(clean_name(fullname), ' ', -1)))
          end
 
  $$
  ;

{% endmacro %}
