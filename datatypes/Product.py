import random
from DataTypeBase import DataTypeBase

class Product(DataTypeBase):
	def __init__(self, fake, useSystemDate=False):
		self.ID = Product.ID
		Product.ID += 1
		self.product_barcode_8 = fake.ean8()
		self.product_barcode_13 = fake.ean13()
		self.price_per_unit = random.randint(1, 1000) * random.random()
		self.description = fake.text(max_nb_chars=200)

	@staticmethod
	def get_headers(self, delimiter="|"):
		return "product ID" + delimiter + \
				"product barcode 8" + delimiter + \
				"product barcode 13" + delimiter + \
				"price per unit" + delimiter + "description"

	def to_record(self, delimiter="|"):
		return str(self.ID) + delimiter + \
				self.product_barcode_8 + delimiter + \
				self.product_barcode_13 + delimiter + \
				str(self.price_per_unit) + delimiter + \
				self.description