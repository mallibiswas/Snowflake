select parse_json($1):_id::string   as offer_id,
       parse_json($1):code::string as offer_code,
       current_date                as asof_date
from {{ most_recent_s3_file_name('_STAGE', 'S3_MONGO_STAGE', '.*/smbsite_offercode.json') }}
