import abc
import json

class DataTypeBase():
	"""
		DataTypeBase is the abstract class that all data types
		inherit from.

		Attributes:
			__metaclass__: a property required by abc module
			ID (int): static property that indicates an object's id
	"""
	__metaclass__ = abc.ABCMeta
	ID = 1
	
	@abc.abstractmethod
	def __init__(self, fake, useSystemDate=False):
		"""
			The fields and values of the data type are defined here
		"""
		return

	@staticmethod
	@abc.abstractmethod
	def get_headers(delimiter="|"):
		"""
			Returns the fields of this data type separated by delimiter
		"""
		return

	@abc.abstractmethod
	def to_record(self, delimiter="|"):
		"""
			Returns the values of this data type separated by delimiter
		"""
		return

	def to_json(self):
		"""
			Returns this data type in json format. Derived data types
			do not need to implement this method.
		"""
		return json.dumps(self.__dict__)
