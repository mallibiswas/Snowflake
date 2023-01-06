SELECT $1:_id:"$oid"::STRING              AS MESSAGESTATS_ID,
       $1:message_id:"$oid"::STRING       AS MESSAGE_ID,
       $1:click_breakdown::VARIANT        AS CLICK_BREAKDOWN,
       $1:offers_stats::VARIANT           AS OFFERS_STATS,
       $1:bounced::INTEGER                AS BOUNCED,
       $1:soft_bounced::INTEGER           AS SOFT_BOUNCED,
       $1:clicked::INTEGER                AS CLICKED,
       $1:clicks::VARIANT                 AS CLICKS,
       $1:conversions::INTEGER            AS CONVERSIONS,
       $1:cumulative_conversions::VARIANT AS CUMULATIVE_CONVERSIONS,
       $1:cumulative_opened::VARIANT      AS CUMULATIVE_OPENED,
       $1:opened::INTEGER                 AS OPENED,
       $1:sent::INTEGER                   AS SENT,
       $1:delivered::INTEGER              AS DELIVERED,
       $1:trackable::INTEGER              AS TRACKABLE,
       $1:unsubscribed::INTEGER           AS UNSUBSCRIBED,
       $1:updated:"$date"::DATETIME       AS UPDATED,
       current_date               AS ASOF_DATE
FROM {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/models_messagestats.json') }}
