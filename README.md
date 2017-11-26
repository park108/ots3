# ots3
Oracle to S3 csv file transfer script

## [settings.conf]<br />
ora_host = 127.0.0.1 _(Your oracle host)_<br />
ora_id = scott _(Your oracle ID)_<br />
ora_password = tiger _(Your oracle password)_<br />
ora_database = orcl _(Your oracle database)_<br />
aws_access_key_id = AKDAIE4K1TAWHGOYPPCQ _(AWS access key id)_<br />
aws_secret_access_key = dpiwiVcjuUxapY6yKfajxshAJpYFSau5B43CheiD _(AWS secret access key)_<br />
aws_s3_bucket = ots3-bucket-name _(S3 bucket name)_<br />
query_file = table_query.sql _(Your query file name)_<br />
output_file_name = test_output _(Your local file name & S3 object key name)_<br />
