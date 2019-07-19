# Import base test
from pysys.basetest import BaseTest
from FloodMonitoringDataHelper import FloodMonitoringDataHelper
from datetime import datetime
import codecs
import csv

class DataImporterBaseTest(BaseTest):

	def __init__ (self, descriptor, outsubdir, runner):
		BaseTest.__init__(self, descriptor, outsubdir, runner)

		# Batching
		self.batch = []
		self.BATCH_COUNT = 1000
		self.total_written = 0

	# Create indexes
	def create_indexes(self, collection):
		self.log.info('Creating indexes')
		collection.create_index('Symbol')
		collection.create_index('Time')

		collection.create_index('group.hours')
		collection.create_index('group.days')


	def getLatestReadings(self, log_messages=True):
		helper = FloodMonitoringDataHelper(self)
		return helper.getLatestReadings(log_messages)


