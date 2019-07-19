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
		db = client.get_database('readings')
		db_ref = client.get_database('ref')
		helper = FloodMonitoringDataHelper(self)

		count = 0
		db.data.drop()
		for station in db_ref.stations.find({'riverName': 'River Thames'}):
			station_id = station['stationReference']
			data = helper.getAllReadings(station_id)
			processed_items = []
			items = data['items']
			for item in items:
				item['date'] = item['dateTime'][:10]
				processed_items.append(item)
				count +=1 
				if count % 100 == 0:
					self.log.info(f'Inserted {count}')
			self.insert_batch(processed_items, db.data)

	def validate(self):
		pass

