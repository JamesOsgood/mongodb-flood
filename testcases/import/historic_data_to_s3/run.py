# Import base test
from DataImporterBaseTest import DataImporterBaseTest
from FloodMonitoringDataHelper import FloodMonitoringDataHelper
from pymongo import MongoClient
from datetime import datetime
import os
import json
import bson.json_util
import boto3

class PySysTest(DataImporterBaseTest):

	def __init__ (self, descriptor, outsubdir, runner):
		DataImporterBaseTest.__init__(self, descriptor, outsubdir, runner)

	def execute(self):

		conn_str = self.project.MONGODB_CONNECTION_STRING_ATLAS.replace("~", "=")
		client = MongoClient(conn_str)
		db = client.get_database('readings')
		
		s3 = boto3.client('s3')
		output_bucket_name = 'jco-datalake'
		output_dir_name = 'water_data'
		s3_output_bucket = boto3.resource('s3').Bucket(output_bucket_name)

		for doc in db.data.aggregate([ { '$sortByCount': '$date' } ]):
			date = doc['_id']

			# Get all data on this date
			measures = db.data.find({'date' : date})
			output_filename = f'{date}T00:00:00Z.json'
			output_path = os.path.join(self.output, output_filename)
			with open(output_path, 'w') as f:
				for value in measures:
					f.write(json.dumps(value, default=bson.json_util.default))

			# Write to s3
			key = output_dir_name + '/' + output_filename
			self.log.info('Uploading %s with key %s' % (output_filename, key) )
			s3_output_bucket.upload_file(Key=key, 
										 Filename=output_path)

	def validate(self):
		pass


