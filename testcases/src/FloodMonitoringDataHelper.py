import requests
from requests.auth import HTTPDigestAuth
import json

from datetime import datetime

'''
Created on 23 Jul 2017

@author: james.osgood
'''

class FloodMonitoringDataHelper(object):
	'''
	Helper for Atlas
	'''
	def __init__(self, parent, log_messages=False):
		
		self.api_base = "http://environment.data.gov.uk/flood-monitoring"
		# self.auth = HTTPDigestAuth(api_user, api_key)
		self.parent = parent

	def getLatestReadings(self, log_messages=False):
		results = self.doRequestGet("%s/data/readings?latest" % self.api_base, log_messages)
		return results

	def getAllReadings(self, station, log_messages=False):
		results = self.doRequestGet("%s/data/readings?stationReference=%s" % (self.api_base, station), log_messages)
		return results

	def getAllStations(self, station, log_messages=False):
		stations = self.doRequestGet("%s/id/stations" % self.api_base, log_messages)
		return stations

	def getAllMeasures(self, log_messages=False):
		measures = self.doRequestGet("%s/id/measures" % self.api_base, log_messages)
		return measures

	def getMeasureDetails(self, url):
		return self.doRequestGet(url)

	def formatDateISO8601(self, date):
		return datetime.strftime(date, '%Y-%m-%dT%H:%M:%SZ')

	def doRequestGet(self, url, log_messages=False):
		if log_messages:
			self.parent.log.info('GET:%s' % url)
		response = requests.get(url)
		if log_messages:
			self.parent.log.info(response.json())
		
		expected_return = 200
		if response.status_code != expected_return:
			self.parent.log.info('API call returned %d, expected %d' % (response.status_code, expected_return))
			raise RuntimeError(response.json())
		else:
			return response.json()

	def doRequestPost(self, url, data, expected_return = 200, log_messages = False):
		if log_messages:
			self.parent.log.info(url)
		response = requests.post(url, auth=self.auth, data=json.dumps(data),headers={'content-type':'application/json', 'accept':'application/json'})
		if log_messages:
			self.parent.log.info(response.json())
		if response.status_code != expected_return:
			self.parent.log.info('API call returned %d, expected %d' % (response.status_code, expected_return))
			raise RuntimeError(response.json())
		else:
			return response.json()

	def doRequestPatch(self, url, data, expected_return = 200, log_messages = False):
		if log_messages:
			self.parent.log.info(url)
		headers = { 'Content-Type' : 'application/json'}
		response = requests.patch(url, auth=self.auth, data=json.dumps(data), headers = headers)
		if log_messages:
			self.parent.log.info(response.json())
		if response.status_code != expected_return:
			self.parent.log.info('API call returned %d, expected %d' % (response.status_code, expected_return))
			raise RuntimeError(response.json())
		else:
			return response.json()

	def doRequestDelete(self, url, expected_return = 202, log_messages = False):
		if log_messages:
			self.parent.log.info(url)
		headers = { 'Content-Type' : 'application/json'}
		response = requests.delete(url, auth=self.auth, headers = headers)
		if log_messages:
			self.parent.log.info(response.json())
		if response.status_code != expected_return:
			self.parent.log.info('API call returned %d, expected %d' % (response.status_code, expected_return))
			raise RuntimeError(response.json())
		else:
			return response.json()
