{{ config(materialized='table') }}

select b.parent_id, b.parent_name, b.business_id, b.business_name, current_timestamp() as created_at

    from {{ seed_or_ref( ref('stg_crm__businessprofile_hierarchy'), 'seed_businessprofile_hierarchy') }} b
    ,
    {{ seed_or_ref( ref('stg_crm__portal_businessprofile'), 'seed_portal_businessprofile') }} p

where b.business_id = p.business_id
      and (b.business_id in (select zenreach_bid

        from {{ seed_or_ref( ref('stg_pos__merchant'), 'seed_merchant') }} 

        ) -- Official Clover integrations
      )
      and b.business_id not in ('5cc1050070cd410001dd9206', '5cc106183691fc0001b37677', '5d1d08822fe83c0001b162c2') -- TODO: These are test businesses that aren't marked properly, fix business profiles
      and p.test_business = FALSE
