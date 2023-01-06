{% macro create_crm_score_function() %}

create or replace function crm_score(fullname_1 string, contact_1 string, fullname_2 string, contact_2 string)
  returns int
  as
  $$

   -- contacts always need to be perfect matches
   least( iff(editdistance(contact_1, contact_2) = 0,0,100)
          , editdistance(ifnull(get_first_name(fullname_1),'') || ifnull(get_last_name(fullname_1),''), strip_pad_email_user(contact_2, 5, '$'))
          , editdistance(ifnull(get_first_name(fullname_1),'') || ifnull(get_middle_initial(fullname_1), '') || ifnull(get_last_name(fullname_1),''), strip_pad_email_user(contact_2, 5, '$'))
          , editdistance(ifnull(get_last_name(fullname_1),'') || ifnull(get_first_name(fullname_1),''), strip_pad_email_user(contact_2, 5, '$')) 
          , editdistance(ifnull(get_last_name(fullname_1),'') || ifnull(get_middle_initial(fullname_1), '') || ifnull(get_first_name(fullname_1),''), strip_pad_email_user(contact_2, 5, '$'))
          , editdistance(ifnull(get_first_name(fullname_1),'') || ifnull(get_last_name(fullname_1),''), strip_pad_fullname(fullname_2, 5, '$'))
          , editdistance(ifnull(get_first_name(fullname_1),'') || ifnull(get_middle_initial(fullname_1), '') || ifnull(get_last_name(fullname_1),''), strip_pad_fullname(fullname_2, 5, '$'))
          , editdistance(ifnull(get_last_name(fullname_1),'') || ifnull(get_first_name(fullname_1),''), strip_pad_fullname(fullname_2, 5, '$'))
         )
 
  $$
  ;

{% endmacro %}
