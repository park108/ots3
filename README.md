# ots3
Oracle to S3 csv file transfer script

[settings.conf]
ora_host = 127.0.0.1 (Your oracle host)
ora_id = scott (Your oracle ID)
ora_password = tiger (Your oracle password)
ora_database = orcl (Your oracle database)
aws_access_key_id = AKDAIE4K1TAWHGOYPPCQ (AWS S3 access key id)
aws_secret_access_key = AKDAIE4K1TAWHGOYPPCQ (AWS secret access key)
aws_s3_bucket = ots3-bucket-name (S3 bucket name)
query_file = table_query.sql (Your query file name)
output_file_name = test_output (Your local file name & S3 object key name)
