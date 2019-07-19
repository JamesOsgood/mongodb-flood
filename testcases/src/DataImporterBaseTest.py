# Import base test
from pysys.basetest import BaseTest
from datetime import datetime
import codecs
import csv

class DataImporterBaseTest(BaseTest):

	def __init__ (self, descriptor, outsubdir, runner):
		BaseTest.__init__(self, descriptor, outsubdir, runner)

		# Batching
		self.batch = []
		self.BATCH_COUNT = 100
		self.total_written = 0

	# Create indexes
	def create_indexes(self, collection):
		self.log.info('Creating indexes')
		collection.create_index('Symbol')
		collection.create_index('Time')

		collection.create_index('group.hours')
		collection.create_index('group.days')

	def insert_batch(self, items, collection):
		count = 0
		for item in items:
			self.batch.append(item)
			count += 1
			if len(self.batch) == self.BATCH_COUNT:
				collection.insert_many(self.batch)
				self.batch = []
				self.log.info(f'Batch: Inserted {count}')
		if len(self.batch) > 0:
			collection.insert_many(self.batch)
			self.log.info(f'Batch: Inserted {count}')

		

