import os, sys, io
import boto3
from boto3.s3.transfer import S3Transfer
from optparse import OptionParser
import configparser
import cx_Oracle
from datetime import datetime
import gzip
import csv

# execution
# python3 code.py

if __name__ == "__main__":

	start_time = datetime.now()

	print ("################################################")
	print ("Oracle to S3")
	
	################################################
	# Parsing setting parameters
	################################################
	config = configparser.ConfigParser()
	config.read("./settings.conf")

	ora_host = config.get("default", "ora_host")
	ora_id = config.get("default", "ora_id")
	ora_password = config.get("default", "ora_password")
	ora_database = config.get("default", "ora_database")

	aws_access_key_id = config.get("default", "aws_access_key_id")
	aws_secret_access_key = config.get("default", "aws_secret_access_key")
	aws_s3_bucket = config.get("default", "aws_s3_bucket")

	query_file = config.get("default", "query_file")
	output_file_name = config.get("default", "output_file_name")


	################################################
	# Query
	################################################
	print ("################################################")
	print ("[Query]")

	ora_host_db = ora_host + "/" + ora_database
	print ("  Connect to oracle database... " + ora_host_db)

	os.environ["NLS_LANG"] = ".AL32UTF8" # for use Unicode
	ora_conn = cx_Oracle.connect(ora_id, ora_password, ora_host_db)
	print ("  Connection established.")

	query = open(query_file, "r").read().strip().strip(";")
	print ("    Load query file << " + query_file)

	cursor = ora_conn.cursor()
	cursor.execute(query)
	print ('    Query executed.')

	query_time = datetime.now();
	print ('* Elapsed time: {}'.format(query_time - start_time))


	################################################
	# Create local file
	################################################
	print ("################################################")
	print ("[Create file]")
	filename = output_file_name + ".csv.gzip"
	print ("  Cursor data to local file >> " + filename)

	file = gzip.open(filename, "wt", encoding="utf-8", newline="\n")
	csv_writer = csv.writer(file, dialect='excel')

	str_data = ""
	str_len = 0
	original_str_length = 0
	row_count = 0

	print ("  Fetch data...")
	fetch_start = datetime.now()
	results = cursor.fetchall()
	row_count = len(results)
	fetch_end = datetime.now()
	print ('    Elapsed time: {}'.format(fetch_end - fetch_start))

	print ("  Convert to CSV...")
	convert_start = datetime.now()
	csv_writer.writerows(results)
	convert_end = datetime.now()
	print ('    Elapsed time: {}'.format(convert_end - convert_start))

	file.close()
	print ("  Close file.")
	cursor.close()
	print ("  Close cursor.")
	ora_conn.close()
	print ("  Close oracle connection.")

	create_file_time = datetime.now();
	print ('* Elapsed time: {}'.format(create_file_time - query_time))


	################################################
	# Send local file to AWS S3
	################################################
	print ("################################################")
	print ("[Transfer file]")
	
	client = boto3.client('s3'
		, aws_access_key_id = aws_access_key_id
		, aws_secret_access_key = aws_secret_access_key)

	print ("  S3 connected.")
	print ("    S3 bucket name: " + aws_s3_bucket)
	
	s3_path = "s3://" + aws_s3_bucket + "/" + filename
	print ("    Transfer... " + filename)
	print ("            >>> " + s3_path)
	transfer = S3Transfer(client)
	transfer.upload_file(filename, aws_s3_bucket, filename)
	print ("    File transfer completed.")

	upload_file_time = datetime.now();
	print ('* Elapsed time: {}'.format(upload_file_time - create_file_time))


	################################################
	# Print result
	################################################
	print ("################################################")
	print ("[Result]")
	print ("  Row count: {:,}".format(row_count));

	compressed_file_size = os.path.getsize(filename)
	print ('  Compressed file size: {:,} bytes'.format(compressed_file_size))
	
	end_time = datetime.now()
	print ('  Elapsed time: {}'.format(end_time - start_time))
	print ("################################################\n\n")