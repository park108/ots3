import os, sys, io
import boto3
from boto3.s3.transfer import S3Transfer
from optparse import OptionParser
import configparser
import cx_Oracle
from datetime import date, datetime
import gzip
import csv, json
from json import dumps

# execution
# python3 ots3.py -c default

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

if __name__ == "__main__":

	start_time = datetime.now()

	print ("################################################")
	print ("Oracle to S3")
	print ("  Strat at {}".format(start_time))
	
	################################################
	# Parsing arguments
	################################################
	parser = OptionParser()
	parser.add_option("-c", "--setting_section", dest="config_section", default="default", help="Config section name")
	(options, args) = parser.parse_args()
	
	
	################################################
	# Parsing setting parameters
	################################################
	config = configparser.ConfigParser()
	config.read("./settings.conf")

	print ("  Setting section: " + options.config_section)
	ora_host = config.get(options.config_section, "ora_host")
	ora_port = config.get(options.config_section, "ora_port")
	ora_id = config.get(options.config_section, "ora_id")
	ora_password = config.get(options.config_section, "ora_password")
	ora_database = config.get(options.config_section, "ora_database")

	aws_access_key_id = config.get(options.config_section, "aws_access_key_id")
	aws_secret_access_key = config.get(options.config_section, "aws_secret_access_key")
	aws_s3_bucket = config.get(options.config_section, "aws_s3_bucket")

	query_file = config.get(options.config_section, "query_file")
	output_file_type = config.get(options.config_section, "output_file_type")
	output_file_name = config.get(options.config_section, "output_file_name")
	delimiter = config.get(options.config_section, "delimiter")


	################################################
	# Query
	################################################
	print ("################################################")
	print ("[Query]")

	ora_host_db = ora_host + ":" + ora_port + "/" + ora_database
	print ("  Connect to oracle database... " + ora_host_db)

	os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"
	ora_conn = cx_Oracle.connect(ora_id, ora_password, ora_host_db
		, nencoding = "AL16UTF16")
	print ("  Connection established.")

	query = open(query_file, "r").read().strip().strip(";")
	print ("    Load query file << " + query_file)

	cursor = ora_conn.cursor()
	cursor.execute(query)
	print ('    Query executed.')

	fetch_start = datetime.now()
	print ("    Fetch data...")
	results = ""
	try:
		results = cursor.fetchall()
	except cx_Oracle.DatabaseError as exc:
		error, = exc.args
		print ("      Oracle-Error-Code:", error.code)
		print ("      Oracle-Error-Message:", error.message)
		fetch_end = datetime.now()
		print ('      Elapsed time: {}'.format(fetch_end - fetch_start))
		print ("* Program terminated")
		sys.exit()
	row_count = len(results)
	fetch_end = datetime.now()
	print ('      Elapsed time: {}'.format(fetch_end - fetch_start))

	query_time = datetime.now();
	print ('* Elapsed time: {}'.format(query_time - start_time))


	################################################
	# Create local file
	################################################
	print ("################################################")
	print ("[Create file]")

	filename = output_file_name
	if output_file_type == 'csv':
		filename += ".csv.gzip"
	elif output_file_type == 'json':
		filename += ".json.gzip"

	print ("  Fetched data to local file >> " + filename)

	file = gzip.open(filename, "wt", encoding="utf8", newline="\n")

	convert_start = datetime.now()

	if output_file_type == 'csv':
		print ("  Convert to CSV...")
		csv_writer = csv.writer(file, dialect='excel', delimiter=delimiter)
		csv_writer.writerows(results)
	elif output_file_type == 'json':
		print ("  Convert to JSON...")
		json_dump = json.dumps(results, default=json_serial, ensure_ascii=False, indent=2)
		file.write(json_dump)

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
