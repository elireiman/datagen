import argparse
from datetime import datetime
from datetime import timedelta
import importlib
import os
import random
import time
import sys

from faker import Factory

"""
Example Usage to create json output of customer records, and to test 
    python dummy_data_gen.py -dt customer -json -br
    python dummy_data_gen.py -dt Sales -json -c 1000

QUEUE UP 8x threads:-------
nohup python dummy_data_gen.py -dt sales -fn sales -c 100000000 &
--> 43K rows / seconds * 8 = 344K / seconds

"""
class TimeIt():
	def __init__(self, run_type, destination, timing_log,rows):
		self.run_type = run_type
		self.date_start = time.time()
		self.date_end = None
		self.seconds_runtime = 0
		self.destination = destination
		self.timing_log = timing_log
		self.rows = rows

	def log_status(self,i):
		dat=time.strftime("%Y-%m-%d %H:%M:%S")
		msg = '{dat} - Rows Generated: {rows}'.format(dat=dat,rows=i)
		sys.stdout.write(msg + '\n')

	def log_end_time(self):
		self.date_end = time.time()
		self.seconds_runtime = self.date_end - self.date_start
		rows_per_sec = int(self.rows / self.seconds_runtime)
		with open(self.destination+'/'+self.timing_log, 'a') as f:
			msg = {
			'run_type': self.run_type
			, 'date_start': datetime.fromtimestamp(self.date_start).strftime('%Y-%m-%d %H:%M:%S')
			, 'date_end': datetime.fromtimestamp(self.date_end).strftime('%Y-%m-%d %H:%M:%S')
			, 'seconds_runtime': round(self.seconds_runtime,1)
			, 'rows': self.rows
			, 'rows_per_sec': rows_per_sec
			}
			f.write(str(msg) + ",\n")
			print(msg)

class DummyDataGen():
	""" DummyDataGen generates dummy data objects.

		Attributes:
			dataType (str): data type of dummy data objects
			startingID (int): the starting id for dummy objects that have auto incrementing ids.
			count (int): the number of dummy data objects to create
			fileName (str): the name of the file that contains the dummy data
			destination (str): the path to the file that contains the dummy data
			timingLog (str): the name of the file that contains time logs
			statusEveryXRecords (int): the checkpoint that progress is printed to console
			benchmarkReport (bool): if true, will generate a benchmark report
			useSystemDate (bool): if true, date property of all dummy data will be set to system time
			json (bool): if true, the dummy objects will be written to file in json format
	"""
	def __init__(self, conf, fake):
		"""
		Args:
			conf (dict): a dictionary containing configuration information for DummyDataGen
			fake (obj:'faker'): a faker instance
		"""
		self.dataType = conf['dataType']
		self.startingID = conf['startingID']
		self.count = conf['count']
		self.fileName = conf['fileName']
		self.destination = conf['destination']
		self.delimiter = conf['delimiter']
		self.timingLog = conf['timingLog']
		self.statusEveryXRecords = conf['statusEveryXRecords']
		self.benchmarkReport = conf['benchmarkReport']
		self.useSystemDate = conf['useSystemDate']
		self.add_date_to_filename = conf['add_date_to_filename']
		self.json = conf['json']
		self.fake = fake

	def generate_dummy_data(self):
		try:
			module = importlib.import_module("datatypes.{}".format(self.dataType))
			dummy_object = getattr(module, self.dataType)
		except ImportError:
			print("There is no data type called {}.".format(self.dataType))
			sys.exit()

		if self.add_date_to_filename:
			dat=time.strftime("%Y%m%d%H%M%S")
			file_name_combined = self.destination+'/'+self.fileName + '_' + dat + '_' + str(random.randint(100000,999999))
		else:
			file_name_combined = self.destination+'/'+self.fileName

		try:
			with open(file_name_combined, 'w') as f:
				timeit = {}
				timeit[self.dataType] = TimeIt(self.dataType, self.destination, self.timingLog, self.count)
				if not self.json:
					f.write(dummy_object.get_headers(self.delimiter) + "\n")
				for i in range(0, self.count):
					temp = dummy_object(self.fake)
					f.write((temp.to_record(self.delimiter) if not self.json else temp.to_json())+ "\n")
					if i%self.statusEveryXRecords == 0:
						timeit[self.dataType].log_status(i)
				timeit[self.dataType].log_end_time()
				#log_end_time()
		except IOError as e:
			print("Did not successfully generate dummy data.")
			print("Destination directory doesn't exist.")
			return

	def generate_benchmark_report(self):
		"""
		For count of 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, calculate and estimate run time and file size.
		Write the results to the file named benchmark-report.txt in the current directory.
		"""
		try:
			with open(self.destination+'/'+'benchmark-report.txt', 'w') as f:
				# Perform tests
				test_cases = [100, 1000, 10000, 100000]
				test_results = {}
				for test_case in test_cases:
					self.count = test_case
					start = time.time()
					self.generate_dummy_data()
					end = time.time()

					elapsed_time_in_sec = end - start
					file_size_in_bytes = os.stat(self.fileName).st_size

					result = {'elapsed time': elapsed_time_in_sec, 'file size': file_size_in_bytes}
					test_results[test_case] = result

				# Write results to file
				f.write("{:20}{:30}{:20}\n".format('Row Count', 'Elapsed Time', 'File Size'))
				for test_case in test_cases:
					f.write("{:20}{:30}{:20}\n".format(str(test_case), str(test_results[test_case]['elapsed time'])+" s", str(test_results[test_case]['file size'])+" B"))
				f.write("{:20}{:30}{:20}\n".format("1,000,000", "~"+str(test_results[100000]['elapsed time']*10/60)+" m", "~"+str(test_results[100000]['file size']*10/1000000)+" MB"))
				f.write("{:20}{:30}{:20}\n".format("10,000,000", "~"+str(test_results[100000]['elapsed time']*100/3600)+" h", "~"+str(test_results[100000]['file size']*100/1000000000)+" GB"))
				f.write("{:20}{:30}{:20}\n".format("100,000,000", "~"+str(test_results[100000]['elapsed time']*1000/3600)+" h", "~"+str(test_results[100000]['file size']*1000/1000000000)+" GB"))
		except IOError as e:
			print("Did not successfully generate benchmark report.")
			print("Destination directory doesn't exist.")
			return

def main():
	args = get_command_line_arguments()
	conf = generate_configuration_dict_from(args)
	ddg = DummyDataGen(conf, Factory.create())
	if args.benchmarkReport is False:
		ddg.generate_dummy_data()
	else:
		ddg.generate_benchmark_report()

def get_command_line_arguments():
	parser = argparse.ArgumentParser()
	parser.add_argument("-dt", "--dataType", help="the data type of your dummy data. current options include sales, customer, product", default="Sales")
	parser.add_argument("-id", "--startingID", help="The starting id for dummy objects that have auto incrementing id", default=1)
	parser.add_argument("-c", "--count", help="the number of records to create", type=int, default=1000)
	parser.add_argument("-fn", "--fileName", help="the name of the flat file that contains dummy data", default="dummy-data.txt")
	parser.add_argument("-d", "--destination", help="the directory destination to store the flat file", default=".")
	parser.add_argument("-delim", "--delimiter", help="the delimiter used to separate the fields of a record", default="|")
	parser.add_argument("-tl", "--timingLog", help="File name for the timing -- appends to a continuous log", default="dummy_data.log")
	parser.add_argument("-r", "--statusEveryXRecords", help="print status every x records", type=int, default=None)
	parser.add_argument("-br", "--benchmarkReport", help="if set, will generate a bench report in the current directory", action="store_true")
	parser.add_argument("-sd", "--useSystemDate", help="if set, will use system date as date value in dummy data", action="store_true")
	parser.add_argument("-json", "--json", help="if set, will output dummy data in json format", action="store_true")
	parser.add_argument("-ad", "--add_date_to_filename", help="Add Date to filename?", type=bool, default=True)
	args = parser.parse_args()
	if args.statusEveryXRecords == None:
		args.statusEveryXRecords = args.count / 10
	return args

def generate_configuration_dict_from(args):
	conf = {}
	conf['dataType'] = args.dataType
	conf['startingID'] = args.startingID
	conf['count'] = args.count
	conf['fileName'] = args.fileName
	conf['destination']  = args.destination
	conf['delimiter'] = args.delimiter
	conf['timingLog'] = args.timingLog
	conf['statusEveryXRecords'] = args.statusEveryXRecords
	conf['benchmarkReport'] = args.benchmarkReport
	conf['useSystemDate'] = args.useSystemDate
	conf['json'] = args.json
	conf['add_date_to_filename'] = args.add_date_to_filename
	return conf

if __name__ == "__main__":
	main()
