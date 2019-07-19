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
		db = client.get_database('ref')

		helper = FloodMonitoringDataHelper(self)
		measures = helper.getAllMeasures()
		data = helper.getAllStations()

		enriched_count = 0
		count = 0
		processed_items = []
		for item in data['items']:
			if 'long' in item:
				long = float(item['long'])
				lat = float(item['lat'])
				coords = [long, lat]
				item['location'] = { 'type' : 'Point', 'coordinates' : [ long, lat ] }

			# Enrich measure data
			if 'measures' in item:
				for measure in item['measures']:
					id = measure['@id']
					if id in measures:
						info = measures[id]
						measure['measure_label'] = info['label']
						measure['measure_notation'] = info['notation']
			enriched_count += 1
			if enriched_count % 10 == 0:
				self.log.info(f'Enriched {enriched_count}')

			processed_items.append(item)
			

		db.stations.drop()
		self.insert_batch(processed_items, db.stations)

	def validate(self):
		pass


