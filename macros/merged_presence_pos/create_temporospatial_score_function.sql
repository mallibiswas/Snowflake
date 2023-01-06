{% macro create_temporospatial_score_function() %}

create or replace function temporospatial_score(fullname_1 string, contact_1 string, fullname_2 string, contact_2 string)
  returns int
  as
  $$

   -- contacts always need to be perfect matches
   least( iff(editdistance(contact_1, contact_2) = 0,0,100)
          -- full first name / last name to email user names are allowed are given more room to differ for temporospatial matches, hence the -2 buffer provided the string is a reasonable length. 
          -- If it's super short full name then it has to be a perfect match.
          -- INPUT carrie isaacson
          -- carrie isaacson vs 
          , editdistance(ifnull(get_first_name(fullname_1),'') || ifnull(get_last_name(fullname_1),''), strip_pad_email_user(contact_2, 5, '$')) 
                - iff(len(strip_pad_email_user(contact_2, 5, '$')) > 8, 2, 0)
          , editdistance(ifnull(get_first_name(fullname_1),'') || ifnull(get_middle_initial(fullname_1), '') || ifnull(get_last_name(fullname_1),''), strip_pad_email_user(contact_2, 5, '$')) 
                - iff(len(strip_pad_email_user(contact_2, 5, '$')) > 8, 2, 0)
          , editdistance(ifnull(get_last_name(fullname_1),'') || ifnull(get_first_name(fullname_1),''), strip_pad_email_user(contact_2, 5, '$')) 
                - iff(len(strip_pad_email_user(contact_2, 5, '$')) > 8, 2, 0)
          , editdistance(ifnull(get_last_name(fullname_1),'') || ifnull(get_middle_initial(fullname_1), '') || ifnull(get_first_name(fullname_1),''), strip_pad_email_user(contact_2, 5, '$')) 
                - iff(len(strip_pad_email_user(contact_2, 5, '$')) > 8, 2, 0)
          , editdistance(ifnull(get_first_name(fullname_1),'') || ifnull(get_last_name(fullname_1),''), strip_pad_fullname(fullname_2, 5, '$')) 
                - iff(len(strip_pad_email_user(contact_2, 5, '$')) > 8, 2, 0)
          , editdistance(ifnull(get_first_name(fullname_1),'') || ifnull(get_middle_initial(fullname_1), '') || ifnull(get_last_name(fullname_1),''), strip_pad_fullname(fullname_2, 5, '$')) 
                - iff(len(strip_pad_email_user(contact_2, 5, '$')) > 8, 2, 0)
          , editdistance(ifnull(get_last_name(fullname_1),'') || ifnull(get_first_name(fullname_1),''), strip_pad_fullname(fullname_2, 5, '$')) 
                - iff(len(strip_pad_email_user(contact_2, 5, '$')) > 8, 2, 0)
          -- first initial + last matches or just a first name match aren't as great, require more than one temporospatial match to create a join.
          -- add 1 to make these matches a little tougher to make it through
          , editdistance( ifnull(get_first_initial(fullname_1),'') || ifnull(get_middle_initial(fullname_1), '') || ifnull(get_last_name(fullname_1),''), strip_pad_email_user(contact_2, 5, '$')) + 1
          , editdistance(ifnull(get_first_name(fullname_1),''), strip_pad_email_user(contact_2, 5, '$')) + 1
         )
 
  $$
  ;

{% endmacro %}
