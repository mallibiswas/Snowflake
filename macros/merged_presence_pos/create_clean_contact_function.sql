{% macro create_clean_contact_function() %}
-- Universal contact clean-up function
-- TODO. removal of periods from email username should only be applied to gmail accounts
create or replace function clean_contact(contact string)
  returns string
  as
  $$
  
    CASE WHEN contact LIKE '%@%' THEN
      lower(regexp_replace(
            split_part(split_part(trim(contact),'@',1), '+',1)
            ,'[^a-zA-Z0-9@_]'
         )
      || '@'
      || regexp_replace(regexp_replace(regexp_replace(regexp_replace(split_part(trim(contact),'@',2)
              ,'gamil|gnail|gmai\.|gmal|gmial|gmaill|gmil|hmail|gmaul|gmaiil|gmaik|gmaio|gmaik|gemail|gmqil|gmsil|ymail', 'gmail')
              , 'hormail|hotmai\.|hotnail|homail|hotmal|hitmail|hotmsil|hotmial|hotmaul|hotamil|htomail|hotmaol|hotmil', 'hotmail')
              , 'tahoo|yahho|yhaoo|yhoo|tahoo|yaoo|yshoo|uahoo|@ahoo\.|yahhoo|yahpp', 'yahoo')
              , '.con$', '.com'))
    ELSE
       iff(lower(regexp_replace(trim(contact),'[^0-9]'))='',NULL,lower(regexp_replace(trim(contact),'[^0-9]')))
    END
    
  $$
  ;
{% endmacro %}
