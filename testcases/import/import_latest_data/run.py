# Import base test
from DataImporterBaseTest import DataImporterBaseTest
from pymongo import MongoClient
from datetime import datetime
import os

class PySysTest(DataImporterBaseTest):

	def __init__ (self, descriptor, outsubdir, runner):
		DataImporterBaseTest.__init__(self, descriptor, outsubdir, runner)

	def execute(self):

		# conn_str = self.project.MONGODB_CONNECTION_STRING_ATLAS.replace("~", "=")
		# client = MongoClient(conn_str)
		# db = client.get_database()

		data = self.getLatestReadings(log_messages=True)
		self.log.info(data)


	def validate(self):
		pass


