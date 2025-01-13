#################################################################
#
#  Copied from onetick example for running otq file in Python
#        ExampleUtil.py
#        
#################################################################
from __future__ import print_function
import os
import datetime
import pyomd
import ExampleUtil

'''
User defined callback that just prints out the input data.
'''

class InfoCallback (pyomd.PythonOutputCallback):
	'''User defined callback that just prints out the input data.'''
	symbol = None
	label = None
	timezone = None
	
	def __init__(self, tz, savePath):
		self.timezone = tz
		pyomd.PythonOutputCallback.__init__(self)
		self.otqSavePath = savePath
	
	#------------------------------------------------------------
	# Overriden methods.
	#------------------------------------------------------------
	
	def replicate(self):
		'''Create a new instance of a callback object'''
		
		cb = InfoCallback(self.timezone)
		cb.symbol = self.symbol
		cb.label = self.label
		
		# Important: You need to call __disown__() here to avoid object be deleted by python,
		# the pyomd library will take care to delete this object.
		return cb.__disown__()
		
	
	def process_callback_label(self, callback_label):
		'''Assign label to this callback object'''
		# print(" Processing callback label ", callback_label)
		self.label = callback_label
	
	def process_symbol_name(self, symbol_name):
		'''Report name of security which ticks are processed '''
		# print(" Processing symbol name ", symbol_name)
		self.symbol = symbol_name
	
	def process_symbol_group_name(self, symbol_group_name):
		'''Report a named group of securities, i.e. portfolio '''
		# print(" Processing symbol group name ", symbol_group_name)
		return None
	
	def process_tick_type(self, tick_type):
		'''Report a tick type of the security ticks which are processed '''
		# print(" Processing tick_type ", tick_type.get_tick_type_spec())
		return None
	
	def process_tick_descriptor(self, tick_descriptor):
		'''Process tick descriptor'''
		return None
		# print(" Processing tick descriptor")
		# for i in range(0, tick_descriptor.get_num_of_fields()):
			# field = tick_descriptor.get_field(i)
			# print("   " + str(field.get_name()) + " type: " + str(field.get_type()) + " size: " + str(field.get_size()))
		
	def process_event(self, tick, time):
		'''Process tick.'''
		# print("=======this is a test=======")
		symb = str(self.symbol)
		data_row = []
		data_row.append(symb)
		dt = ExampleUtil.Date2YYYMMDDhhmmss(time, self.timezone)
		dt = dt[0:4]+'/'+dt[4:6]+'/'+dt[6:8]+' '+dt[8:10]+':'+dt[10:12]+':'+dt[12:]
		data_row.append(dt)
		for i in range(0, tick.get_num_of_fields()):
			type = tick.get_type(i)
			if (type == pyomd.DataType.TYPE_INT8) or (type == pyomd.DataType.TYPE_INT16) or (type == pyomd.DataType.TYPE_INT32) or (type == pyomd.DataType.TYPE_UINT32) or (type == pyomd.DataType.TYPE_TIME32):
				data_row.append(str(tick.get_int(i))),
			elif type == pyomd.DataType.TYPE_INT64:
				data_row.append(str(tick.get_int64(i))),
			elif type == pyomd.DataType.TYPE_STRING:
				tick.get_string(i);
				data_row.append(str(tick.get_string(i))),
			elif (type == pyomd.DataType.TYPE_FLOAT) or (type == pyomd.DataType.TYPE_DOUBLE) :
				data_row.append("{:.7f}".format(tick.get_double(i))),
			elif (type == pyomd.DataType.TYPE_TIME_MSEC64) or (type == pyomd.DataType.TYPE_TIME_NSEC64):
				msec64 = tick.get_int64(i);
				sec = msec64 //1000;
				msec = msec64 % 1000;
				t = ExampleUtil.Timestamp2YYYMMDDhhmmss(sec,msec,self.timezone);
				t = t[0:4]+'/'+t[4:6]+'/'+t[6:8]+' '+t[8:10]+':'+t[10:12]+':'+t[12:];
				data_row.append(t),
			else : 
				data_row.append(str(tick.field_as_string(i))),
		with open(os.path.join(self.otqSavePath, symb.replace("::","__"), ".csv"), "a+") as stor:
			stor.write(",".join(data_row) + '\n')
		# print(" Processing tick: "+ dt + " " + str(self.label) + " " + str(self.symbol))
		# for i in range(0, tick.get_num_of_fields()):
			# type = tick.get_type(i)
			# if (type == pyomd.DataType.TYPE_INT8) or (type == pyomd.DataType.TYPE_INT16) or (type == pyomd.DataType.TYPE_INT32) or (type == pyomd.DataType.TYPE_UINT32) or (type == pyomd.DataType.TYPE_TIME32):
				# print(tick.get_int(i), " "),
			# elif type == pyomd.DataType.TYPE_INT64:
				# print(tick.get_int64(i), " "),
			# elif type == pyomd.DataType.TYPE_STRING:
				# tick.get_string(i);
				# print(tick.get_string(i), " "),
			# elif (type == pyomd.DataType.TYPE_FLOAT) or (type == pyomd.DataType.TYPE_DOUBLE) :
				# print(tick.get_double(i),  " "),
			# elif (type == pyomd.DataType.TYPE_TIME_MSEC64) or (type == pyomd.DataType.TYPE_TIME_NSEC64):
				# msec64 = tick.get_int64(i);
				# sec = msec64 //1000;
				# msec = msec64 % 1000;
				# t = ExampleUtil.Timestamp2YYYMMDDhhmmss(sec,msec,self.timezone);
				# print (t, " " ),
			# else : 
				# print(tick.field_as_string(i) + " "),
		# print("")
	
	def process_sorting_order(self, sorted_by_time_flag):
		'''Process sorting order '''
		# print(" Processing sorting order: sorted_by_time flag is ")
		# if 1 == sorted_by_time_flag :
			# print("not "),
		# print("set")
		return None
	
	def process_data_quality_change(self, symbol_name, data_quality, time):
		'''Report data quality changes'''
		# print(" Processing data_quality_change: symbol: " + str(symbol_name) + " data quality type: "),
		# print(data_quality),
		# print(" time: ", time)
		return None
	
	def done(self):
		'''Called after all the ticks were submitted'''
		# print("Done " + str(self.symbol))
		return None
	
	def process_error(self, error_code, error_msg):
		'''Error processing'''
		# print("Processing error: ", error_msg)
		return None
	

