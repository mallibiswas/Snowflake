{% macro create_merge_pos_presence_functions() %}

{{ create_clean_contact_function() }}

{{ create_null_string_to_null_function() }}

{{ create_clean_name_function() }}

{{ create_education_email_function() }}

{{ create_generic_email_function() }}

{{ create_get_email_domain_function() }}

{{ create_get_email_user_function() }}

{{ create_get_first_initial_function() }}

{{ create_get_first_name_function() }}

{{ create_get_last_name_function() }}

{{ create_get_middle_initial_function() }}

{{ create_get_last_initial_function() }}

{{ create_get_initials_function() }}

{{ create_strip_pad_email_user_function() }}

{{ create_strip_pad_fullname_function() }}

{{ create_strip_pad_function() }}

{{ create_temporospatial_score_function() }}

{{ create_zenreach_email_function() }}

{{ create_crm_score_function() }}

{% endmacro %}
