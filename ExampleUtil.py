#################################################################
#
#  Copied from onetick example for running otq file in Python
#        ExampleUtil.py
#        
#################################################################
from __future__ import print_function
import sys
import datetime
import time
import calendar
import pyomd

'''
Set of classes and functions to use in omd examples
'''

class OmdExampleException(pyomd.OneTickException):
	'''
	Exception thrown by OMD examples.
	'''
	
	def __init__(self, message):
		'''
		Construct object and pass parameter to the base class
		'''
		pyomd.OneTickException.__init__(self, message)
	

class CmdLine:
	'''
	Simple command line parser.
	This class parses command line given in form app.exe [option]...[option],
	where option is -opt_name [arg1]...[arg2].
	'''
	def __init__(self, argv):
		'''Parse command line and construct object.'''
		
		self.options = {}
		param = ""
		for i in range(0, len(argv)):
			argvi = argv[i]
			if argvi[0] == '-':
				param = argvi[1:]
				if param not in self.options :
					self.options[param] = []
			else:
				if len(param) == 0:
					continue
				self.options[param].append(argvi)
	
	#------------------------------------------------------------
	# Member functions
	#------------------------------------------------------------
	def getParam(self, param_name, is_mandatory):
		'''
		Get information about parameter.
		Throw object of class CmdLine::Exception if mandatory parameter is missing.
		param_name    - Name of parameter without '-'.
		is_mandatory  - Set to true for mandatory parameter.
		return
		-1 parameter is missing (for optional parameters) 
		0  parameter is a switch
		>0 number of values associated with given parameter
		'''
		
		if param_name in self.options:
			return len(self.options[param_name])
		else:
			if is_mandatory == 1:
				raise OmdExampleException("Mandatory parameter '" + str(param_name) + "' is missing")
			else:
				return -1
	
	def getValue(self, param_name, n = 0, default = None):
		'''
		Retrieve the n-th value of parameter.
		Throw object of class OmdExampleException if parameter or value is missing and
		default is None. If Default is passed then default value is returned.
		param_name - Name of parameter without '-'.
		 n          - Index in array of associated values started from 0. 
		 default    - This value will be returned if parameter or value is missing. 
		 return     - The n-th value of parameter.
		'''
		if param_name in self.options:
			v = self.options[param_name]
			if n >= 0 and n < len(v):
				return v[n]
		if default != None :
			return default
		else :
			raise OmdExampleException("Value for parameter '" + str(param_name) + "' is missing")
	
	def __str__(self):
		'''Output object to the standart output'''
		return self.options.__str__()
	

def YYYMMDDhhmmss2Date(time, tz):
	'''Convert time in form YYYYMMHHhhmmss[.qqq] to Date'''
	try:
		parts = time.split('.')
		sec = pyomd.YYYYMMDDhhmmss_to_gmt_seconds(int(parts[0]), tz)
		temp = datetime.datetime.utcfromtimestamp(sec)
		if len(parts)>1:
			msec = int(parts[1])*1000
			date=temp.replace(microsecond=msec)
			'''print(parts[0]," ",parts[1]," ",msec," ",date)'''
		else:
			msec = 0
			date=temp.replace(microsecond=msec)
			'''print(parts[0]," ",msec," ",date)'''
	except Exception as err:
		print ("Exception happened in YYYMMDDhhmmss2Date: ", format(err))
		raise OmdExampleException("Invalid time specification. Time is expected to be presented as YYYYMMDDhhmmss")
	return date

def Date2YYYMMDDhhmmss(dt, tz):
	'''Convert Date to time in form YYYYMMHHhhmmss[.qqq] in given tz'''
	timestamp = calendar.timegm(dt.utctimetuple())
	tt = pyomd.gmt_seconds_to_YYYYMMDDhhmmss(timestamp,tz)
	msec = dt.microsecond//1000
	s = str(tt)+'.'+str(msec)
	return s
	
def Timestamp2YYYMMDDhhmmss(ts, msec, tz):
	'''Convert unix timestamp to time in form YYYYMMHHhhmmss[.qqq] in given tz'''
	tt = pyomd.gmt_seconds_to_YYYYMMDDhhmmss(ts,tz)
	s = str(tt)+'.'+str(msec)
	return s
