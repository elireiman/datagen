import argparse
import importlib
import os
import random
import time
import sys
from datetime import datetime
from datetime import timedelta
from faker import Factory

"""
v20170418a
Example Usage to create json output of customer records, and to test 
    python dummy_data_gen.py -dt customer -json -br
    python ~/github/datagen/dummy_data_gen.py -dt Sales -json -c 1000 -sd -d ~/tmp -idf ~/id_file
	python /efs/datagen/dummy_data_gen.py -dt Sales -d '/efs/dummydata' -c 100000 -sd  -idf ~/id_file
	nohup python dummy_data_gen.py -dt sales -fn sales -c 100000000 &
		--> 43K rows / seconds * 8 = 344K / seconds
		--> 80K rows / sec on single micro instance on EFS (while bursting)

	python /efs/datagen/dummy_data_gen.py -dt Customer -d '/efs/dummydata' -c 1000 -sd  -id 1
"""

class TimeIt():
	def __init__(self, run_type, destination, timing_log,rows,filename):
		self.run_type = run_type
		self.date_start = time.time()
		self.date_end = None
		self.seconds_runtime = 0
		self.destination = destination
		self.timing_log = timing_log
		self.rows = rows
		self.filename = filename

	def log_status(self,i):
		dat=time.strftime("%Y-%m-%d %H:%M:%S")
		msg = '{dat} - Rows Generated: {rows}'.format(dat=dat,rows=i)
		sys.stdout.write(msg + '\n')

	def log_end_time(self):
		self.date_end = time.time()
		self.seconds_runtime = self.date_end - self.date_start
		rows_per_sec = int(self.rows / self.seconds_runtime)
		with open(os.path.join(self.destination,self.timing_log), 'a') as f:
			msg = {
			'run_type': self.run_type
			, 'date_start': datetime.fromtimestamp(self.date_start).strftime('%Y-%m-%d %H:%M:%S')
			, 'date_end': datetime.fromtimestamp(self.date_end).strftime('%Y-%m-%d %H:%M:%S')
			, 'seconds_runtime': round(self.seconds_runtime,1)
			, 'rows': self.rows
			, 'rows_per_sec': rows_per_sec
			, 'filename': self.filename
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
		self.id_file_for_start = conf['id_file_for_start']
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
		self.move_file_to_child_dir = conf['move_file_to_child_dir']
		self.fake = fake

	def generate_dummy_data(self):
		try:
			module = importlib.import_module("datatypes.{}".format(self.dataType))
			dummy_object = getattr(module, self.dataType)
			dummy_object.ID = self.startingID
		except ImportError:
			print("There is no data type called {}.".format(self.dataType))
			sys.exit()

		if self.add_date_to_filename:
			dat=time.strftime("%Y%m%d%H%M%S")
			file_name_combined = self.dataType + '_' + dat + '_' + str(random.randint(100000,999999))
		else:
			file_name_combined = self.fileName

		try:
			with open(os.path.join(self.destination,file_name_combined), 'w') as f:
				timeit = {}
				timeit[self.dataType] = TimeIt(self.dataType, self.destination, self.timingLog, self.count, file_name_combined)
				if not self.json:
					f.write(dummy_object.get_headers(self.delimiter) + "\n")
				end_id = self.startingID+self.count
				for i in range(self.startingID, end_id):
					temp = dummy_object(self.fake, self.useSystemDate)
					f.write((temp.to_record(self.delimiter) if not self.json else temp.to_json())+ "\n")
					if i%self.statusEveryXRecords == 0:
						timeit[self.dataType].log_status(i-self.startingID)
				timeit[self.dataType].log_end_time()

			#Move file to child directory
			src = os.path.join(self.destination,file_name_combined)
			dest = os.path.join(self.destination,self.move_file_to_child_dir,file_name_combined)
			print 'Moving completed file to child directory: {dest}'.format(dest=dest)
			os.rename(src, dest)

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
			with open(os.path.join(self.destination,'benchmark-report.txt'), 'w') as f:
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

def set_starting_id(startingID,id_file_for_start):
	if id_file_for_start:
		with open(id_file_for_start,'r') as f:
			starting_id = int(f.readline().strip())
	else:
		starting_id = startingID
	return starting_id

def get_command_line_arguments():
	parser = argparse.ArgumentParser()
	parser.add_argument("-dt", "--dataType", help="the data type of your dummy data. current options include sales, customer, product", default="Sales")
	parser.add_argument("-id", "--startingID", help="The starting id for dummy objects that have auto incrementing id", type=int, default=1)
	parser.add_argument("-idf", "--id_file_for_start", help="ID File containing the starting ID.  This overrides -id setting", type=str, default=None)
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
	parser.add_argument("-mv", "--move_file_to_child_dir", help="Move file to child directory after complete", default='complete')
	args = parser.parse_args()
	if args.statusEveryXRecords == None:
		args.statusEveryXRecords = args.count / 10
	return args

def generate_configuration_dict_from(args):
	conf = {}
	conf['dataType'] = args.dataType
	conf['startingID'] = args.startingID
	conf['id_file_for_start'] = args.id_file_for_start
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
	conf['move_file_to_child_dir'] = args.move_file_to_child_dir
	conf['id_file_for_start'] = args.id_file_for_start
	return conf

def main():
	args = get_command_line_arguments()
	conf = generate_configuration_dict_from(args)
	starting_id = set_starting_id(conf['startingID'],conf['id_file_for_start'])
	conf['startingID'] = starting_id
	print 'Starting at ID: {starting_id}'.format(starting_id=starting_id)
	ddg = DummyDataGen(conf, Factory.create())
	if args.benchmarkReport is False:
		ddg.generate_dummy_data()
	else:
		ddg.generate_benchmark_report()
	final_id = starting_id + args.count
	cmd = "echo '{}' > ~/id_file".format(final_id)
	print cmd
	os.system(cmd)

if __name__ == "__main__":
	main()
