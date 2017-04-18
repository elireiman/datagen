import random
from datetime import datetime
from datetime import timedelta
from DataTypeBase import DataTypeBase
import json

class Sales(DataTypeBase):
	def __init__(self, fake, useSystemDate=True):
		self.ID = Sales.ID
		Sales.ID += 1
		self.cust_ID = random.randint(1,100000000)
		self.product_ID = random.randint(1,100000000)
		self.total_dollar_amount = round(random.randint(1,1000) * random.random(),2)
		if useSystemDate:
			self.transaction_date = str(datetime.now())
		else:
			self.transaction_date = str(datetime.now() - timedelta(random.randint(0,3650))) # 3650 days or equivalently 10 years

	@staticmethod
	def get_headers(delimiter="|"):
		return "sales id" + delimiter + \
				"customer id" + delimiter + \
				"product id" + delimiter + \
				"total dollar amount" + delimiter + "transaction date"

	def to_record(self, delimiter="|"):
		return 	str(self.ID) + delimiter + \
				str(self.cust_ID) + delimiter + \
				str(self.product_ID) + delimiter + \
				str(self.total_dollar_amount) + delimiter + \
				self.transaction_date

	def to_json(self):
		return json.dumps(self.__dict__)