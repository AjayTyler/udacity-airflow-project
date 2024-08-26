#!/bin/bash

# AWS CREDENTIALS
# 
# You'll need to replace the string below with your own URI. Be sure
# that it includes parameters for `role_arn`. If you're unsure how to
# format the URI to include that info, just enter it manually via the
# UI in Airflow, then run `airflow connections get redshift -o json`
# and copy the value for "get_uri" in the resulton JSON.
airflow connections add aws_credentials --conn-uri 'aws://AWS-URI-HERE'

# REDSHIFT CREDENTIALS
# 
# Similar to above, we'll add our Redshift info here.
airflow connections add redshift --conn-uri 'postgres://REDSHIFT-URI-HERE'

# VARIABLES
# 
# Change `your-bucket` to whatever is the name of your
# S3 bucket. You only need to include the name--our
# operators etc. will take care of the rest of the
# formatting. So, if your bucket is `s3://mighty-mouse/`
# then all you need to put is `mighty-mouse`.
airflow variables set s3_bucket mighty-mouse

# For convenience, we'll also use variable to track our log
# and song data. I have mine in the root directory of my s3
# bucket, so if yours is nested you'll want to do something
# like `dataflows/log-data` to ensure the DAG will run.
airflow variables set s3_log_data log-data
airflow variables set s3_song_data song-data

# Finally, set the region to reflect your environment.
airflow variables set s3_region us-east-1
