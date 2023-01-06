{{ config(materialized='table') }}

select s.*, current_timestamp() as created_at

    from {{ seed_or_ref( ref('stg_presence__finished_sightings'), 'seed_finished_sightings') }} s

    join {{ seed_or_ref( ref('stg_crm__businessprofile_hierarchy'), 'seed_businessprofile_hierarchy') }} b on s.business_id = b.business_id

join {{ref('mart_merged_presence_pos__pos_locations')}} pos_locs on s.business_id = pos_locs.business_id
where s.contact_info is not null
    and start_time > '2019-01-01' -- hardcoding, or this needs to be restrutured as a UDTF
    and classification <> 'NOTHUMAN'
