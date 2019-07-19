# Import base test
from DataImporterBaseTest import DataImporterBaseTest
from FloodMonitoringDataHelper import FloodMonitoringDataHelper
from pymongo import MongoClient
from datetime import datetime
import os

class PySysTest(DataImporterBaseTest):

	def __init__ (self, descriptor, outsubdir, runner):
		DataImporterBaseTest.__init__(self, descriptor, outsubdir, runner)

	def execute(self):

		conn_str = self.project.MONGODB_CONNECTION_STRING_ATLAS.replace("~", "=")
		client = MongoClient(conn_str)
		db = client.get_database('latest')

		helper = FloodMonitoringDataHelper(self)

		data = helper.getLatestReadings()
		items = data['items']
		db.data.drop()
		self.insert_batch(items, db.data)

	def validate(self):
		pass


