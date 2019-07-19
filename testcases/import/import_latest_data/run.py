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
		db = client.get_database('data')

		helper = FloodMonitoringDataHelper(self)

		measures = helper.getAllMeasures()
		data = helper.getLatestReadings()
		items = data['items']
		processed_items = []
		for item in items:
			measure = item['measure']
			item['stationReference'] = measures[measure]['stationReference']
			processed_items.append(item)

		db.latest.drop()
		self.insert_batch(processed_items, db.latest)

	def validate(self):
		pass


