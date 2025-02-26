from __future__ import print_function
import os
import sys
import threading
import datetime
import time
import yaml
import pyomd
import ExampleUtil
import InfoCallback

from stock_analysis import StockAnalysis

from multiprocessing.dummy import Pool as ThreadPool

with open("TICKMAN/config.yaml") as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)
        sys.exit(1)
        
for i in range(12):
    os.makedirs(config[ list( config.keys() )[i] ], exist_ok=True)

# TODO: Known Problem
# Some entries contain errors in their millisecond values. 
# Specifically, entries with leading 0s in the millisecond field are incorrectly transformed, 
# with the leading 0s becoming trailing zeros.
# e.g., the correct millisecond value "012" is erroneously recorded as "120"
# *** This issue does not impact the analysis at the current level of precision.


def add_header(fname, root = "O:/180901_211231_TRD_OFF/", store = "O:/180901_211231_TRD_OFF_HEADER/", header = 'table_header.csv'):
    header = ''
    with open(header, 'r') as f:
        for l in f:
            header = l
            
    with open(os.path.join(root, fname), 'r') as f:
        with open(os.path.join(store, fname), 'w+') as fs:
            fs.write(header)
            for idx, l in enumerate(f):
                fs.write(str(idx+1)+","+l)

def prepare_otqs(symbolList, save_root, otq_template = "single_stock_template.otq"):
    _list = []
    with open(symbolList, 'r') as f:
        for l in f:
            l = l.strip()
            _list.append(l)
    
    template = ""
    with open(otq_template, 'r') as f:
        template = f.read()
    
    for _symb in _list:
        with open(os.path.join(save_root, _symb.replace("::", "__"), '.otq'), 'w+') as f:
            stock_otq = template.replace("{DB:SYMBOL}", _symb)
            f.write(stock_otq)
            
# thread pool executor
class MultiThreadExecutor():
    def __init__(self, op):
        self.op = op
    
    def opWrap(self, params):
        # Initialization
        droppingInds = self.op(params)
        return droppingInds

    def calculateParallel(self, paraList, threads=2):
        pool = ThreadPool(threads)
        droppingInds = pool.map(self.opWrap, paraList)
        pool.close()
        pool.join()
        return droppingInds

####################################################################################
# copied from onetick example for running otq file for Python
#   examples/python/OtqQueryExample.py
####################################################################################
class RequestCancellation (threading.Thread):
    
    def __init__(self, h, t):
        threading.Thread.__init__(self)
        self.handle = h
        self.timeout = t
        self.interrupted = False
    
    def setSubscriptionId(self, conn, id):
        self.connection = conn
        self.subscription_id = id
    
    def run(self):
        try :
            
            # There is no way to kill this thread, so we check whether thread is interrupted to exit
            i = 0;
            while (False == self.interrupted) and (i < self.timeout):
                i += 5
                time.sleep(5)
            
            if self.interrupted:
                print("Exiting with Interruption.")
                return 
            
            if (self.subscription_id != None) and (len(self.subscription_id) != 0):
                print("Cancelling query with subscription id:" + str(self.subscription_id))
                pyomd.RequestGroup.cancel_running_query(self.connection, self.subscription_id)
            else:
                print("Cancelling query using handle: ", self.handle)
                self.handle.cancel_query()
        except Exception as err :
            print ("Exception happened in Cancellation thread: ", format(err))

        print("Exiting Canceller.")
    
    def interrupt(self):
        self.interrupted = True

class OtqQueryExample:
    
    def __init__(self, savePath):
        self.savePath = savePath
    
    def otqExample(self, options):
        '''Construct,modify and evaluate the query from the existing OTQ file.'''
#         print("Options:")
        
        # Connect to context as logged in user. "DEFAULT" context is used if not set in command line. 
        context = options.getValue("context", 0, "DEFAULT")
#         print("context = " + context)
        
        # Create object to evaluate query from OTQ file
        otq_file = options.getValue("otq_file")
        print("otq_file = " + str(otq_file));
        otq_query = pyomd.OtqQuery(otq_file)
        
        # Processing symbols
        symbols = pyomd.StringCollection()
        
        # From command line
        symbol_count = options.getParam("symbol", False)
        if symbol_count > 0 :
#             print("symbol = "),
            for i in range(0, symbol_count):
                symbol_i = options.getValue("symbol", i)
                symbols.push_back(symbol_i)
#                 print (symbol_i, " "),
#             print("")
        
        # From symbol file
        value = options.getValue("symbol_file", 0, "");
        if len(value) != 0 :
#             print("symbol_file = " + value)
            file = open(value, 'r')
            for symbol in file:
                symbol = symbol.rstrip('\n')
                if symbol[0] != '#':
                    symbols.push_back(symbol)
            file.close()
        
        if len(symbols) != 0 :
            otq_query.set_symbols(symbols);
        
        # Processing symbol date
        value = options.getValue("symbol_date", 0, "")
        if len(value) != 0:
#             print("symbol_date = " + value)
            symbol_date = int(value)
            otq_query.set_symbol_date(symbol_date)
        
        # Processing start and end time
        timezone = options.getValue("timezone",0,"UTC")
#         print("timezone = " + timezone)
        value = options.getValue("start", 0, "")
        if len(value) != 0:
#             print("start = " + value)
            d = ExampleUtil.YYYMMDDhhmmss2Date(value, timezone)
            otq_query.set_start_time(d)
        
        value = options.getValue("end", 0, "")
        if len(value) != 0:
#             print("end = " + value)
            d = ExampleUtil.YYYMMDDhhmmss2Date(value, timezone)
            otq_query.set_end_time(d)
        
        # Processing "apply_times_daily" option
        value = options.getValue("apply_times_daily", 0, "")
        if len(value) != 0:
            if value.lower() == "true" :
#                 print("apply_times_daily = True")
                otq_query.set_apply_times_daily_flag(True, timezone)
            
            elif value.lower() == "false" :
#                 print("apply_times_daily = False")
                otq_query.set_apply_times_daily_flag(False, timezone)
            else:
                raise ExampleUtil.OmdExampleException("Invalid value " + str(value) + " for option apply_times_daily")
            
        # Processing "running query" option
        value = options.getValue("running_query", 0, "")
        if len(value) != 0 :
            running_query_properties = pyomd.RunningQueryProperties()
            if value.lower() == "true":
#                 print("running_query = True")
                otq_query.set_running_query_properties(True,running_query_properties)
            elif value.lower() == "false" :
#                 print("running_query = False")
                otq_query.set_running_query_properties(False, running_query_properties)
            else :
                raise ExampleUtil.OmdExampleException("Invalid value " + str(value) + " for option running_query")
        
        # Processing query parameters
        n_val = options.getParam("param", False)
        if n_val > 0 :
#             print("param ="),
            params = pyomd.otq_parameters_t()
            for i in range(0, n_val) :
                value = options.getValue("param", i)
                param = value.split("=")
                if len(param) == 2:
                    params.set(param[0], param[1])
#                     print(" " + str(param[0]) + "=" + str(param[1])),
                else:
                    raise ExampleUtil.OmdExampleException("Invalid value " + str(value) + " for option param");
            
            otq_query.set_otq_parameters(params)
#             print("")
        
        # Processing timeout
        value = options.getValue("timeout", 0, "")
        timeout = 0
        if len(value) != 0 :
#             print("timeout = " + value)
            timeout = int(value)
        
        
        conn = pyomd.Connection()
        conn.connect(context)
        
        # Create user callback object
        cb = InfoCallback.InfoCallback(timezone, self.savePath)
        
        # Evaluate query
#         print("Starting query evaluation")
        
        if timeout > 0 :
            request_groups_with_tip = pyomd.RequestGroupsWithTIP()
            otq_query.parse(conn)
            otq_query.extract_queries(request_groups_with_tip, cb)
            
            r_group_count = len(request_groups_with_tip)
#             print ("RequestGroup count = ", r_group_count)
            for i in range(0, r_group_count):
                request_group_with_tip = request_groups_with_tip[i]
                request_group = request_group_with_tip.get_request_group()
                time_interval_prop = request_group_with_tip.get_time_interval_properties()
                
                ch = pyomd.QueryCancellationHandle.create_instance()
                req_cancel = RequestCancellation(ch, timeout)
                if time_interval_prop.get_running_query_flag :
                    subscription_id = time_interval_prop.get_running_query_properties().get_subscription_id()
#                     print("subscription_id = " + subscription_id)
                    req_cancel.setSubscriptionId(conn, subscription_id)
                
                req_cancel.start()
                request_group.process_requests(conn, ch, time_interval_prop)
                
                if req_cancel.is_alive():
                    req_cancel.interrupt()
                
                pyomd.QueryCancellationHandle.destroy_instance(ch)
            
        else :
            pyomd.RequestGroup.process_otq_file(otq_query, cb, conn)
            
off_symbol_list = config['off_symbol_list']
off_otq_root = config['off_otq_root']
off_verify_otq_root = config['off_verify_otq_root']
prepare_otqs(off_symbol_list, off_otq_root, "single_stock_template.otq")
prepare_otqs(off_symbol_list, off_verify_otq_root, "num_of_trades_template.otq")

on_symbol_list = config['on_symbol_list']
on_otq_root = config['on_otq_root']
on_verify_otq_root = config['on_verify_otq_root']
prepare_otqs(on_symbol_list, on_otq_root, "single_stock_template.otq")
prepare_otqs(on_symbol_list, on_verify_otq_root, "num_of_trades_template.otq")

off_header_root = config['off_header_root']
on_header_root = config['on_header_root']

off_verify_root = config['off_verify_root']
on_verify_root = config['on_verify_root']

off_download_root = config['off_download_root']
on_download_root = config['on_download_root']

compute_root = config['compute_root']
complete_root = config['complete_root']

randomState = config['randomState']
nb_compute_workers = config['nb_compute_workers']

def run_stock_analysis_warp(on_off_symbol):
    
    on_symbol, off_symbol = on_off_symbol
    assert on_symbol.split("::")[-1] == off_symbol.split("::")[-1]
    
    on_symbol = on_symbol.replace("::","__")
    off_symbol = off_symbol.replace("::","__")
    
    success = False
    while(not success):
        omdlib = pyomd.OneTickLib(None)
        
        if not os.path.exists(os.path.join(on_download_root, on_symbol+'.csv')):
            optstrs = sys.argv+['-otq_file', os.path.join(on_otq_root, on_symbol+'.otq'), '-timezone', 'EST5EDT']
            options = ExampleUtil.CmdLine(optstrs)
            example = OtqQueryExample(on_download_root)
            example.otqExample(options)
        
        exist_on = True
        if not os.path.exists(os.path.join(on_download_root, on_symbol+'.csv')):
            exist_on = False
        
        if os.path.exists(os.path.join(on_verify_root, on_symbol+'.csv')):
            os.remove(os.path.join(on_verify_root, on_symbol+'.csv'))
        optstrs = sys.argv+['-otq_file', os.path.join(on_verify_otq_root, on_symbol+'.otq'), '-timezone', 'EST5EDT']
        options = ExampleUtil.CmdLine(optstrs)
        example = OtqQueryExample(on_verify_root)
        example.otqExample(options)
        
        on_downloadLineCount = 0
        if exist_on:
            with open(os.path.join(on_download_root, on_symbol+'.csv'), 'rb') as dataf:
                on_downloadLineCount = sum(1 for line in dataf)

        on_verifyCount = 0
        with open(os.path.join(on_verify_root, on_symbol+'.csv'), 'r') as countf:
            on_verifyCount = float(countf.read().strip().split(',')[-1])
        
        if (not exist_on) and (on_verifyCount == 0):
            with open(os.path.join(complete_root, off_symbol), 'w+') as f:
                f.write('4')
            return 4
        
        if not os.path.exists(os.path.join(off_download_root, off_symbol+'.csv')):
            optstrs = sys.argv+['-otq_file', os.path.join(off_otq_root, off_symbol+'.otq'), '-timezone', 'EST5EDT']
            options = ExampleUtil.CmdLine(optstrs)
            example = OtqQueryExample(off_download_root)
            example.otqExample(options)

        exist_off = True
        if not os.path.exists(os.path.join(off_download_root, off_symbol+'.csv')):
            exist_off = False
        
        if os.path.exists(os.path.join(off_verify_root, off_symbol+'.csv')):
            os.remove(os.path.join(off_verify_root, off_symbol+'.csv'))
        optstrs = sys.argv+['-otq_file', os.path.join(off_verify_otq_root, off_symbol+'.otq'), '-timezone', 'EST5EDT']
        options = ExampleUtil.CmdLine(optstrs)
        example = OtqQueryExample(off_verify_root)
        example.otqExample(options)

        off_downloadLineCount = 0
        if exist_off:
            with open(os.path.join(off_download_root, off_symbol+'.csv'), 'rb') as dataf:
                off_downloadLineCount = sum(1 for line in dataf)

        off_verifyCount = 0
        with open(os.path.join(off_verify_root, off_symbol+'.csv'), 'r') as countf:
            off_verifyCount = float(countf.read().strip().split(',')[-1])
            
        if (not exist_off) and (off_verifyCount == 0):
            with open(os.path.join(complete_root, off_symbol), 'w+') as f:
                f.write('5')
            return 5
            
        if ( (on_downloadLineCount == on_verifyCount) and (off_downloadLineCount == off_verifyCount) ):
            success = True
        else:
            assert os.path.exists(os.path.join(on_download_root, on_symbol+'.csv'))
            os.remove(os.path.join(on_download_root, on_symbol+'.csv'))
            assert os.path.exists(os.path.join(on_verify_root, on_symbol+'.csv'))
            os.remove(os.path.join(on_verify_root, on_symbol+'.csv'))
            assert os.path.exists(os.path.join(off_download_root, off_symbol+'.csv'))
            os.remove(os.path.join(off_download_root, off_symbol+'.csv'))
            assert os.path.exists(os.path.join(off_verify_root, off_symbol+'.csv'))
            os.remove(os.path.join(off_verify_root, off_symbol+'.csv'))
            
    add_header(on_symbol+'.csv', on_download_root, on_header_root)
    add_header(off_symbol+'.csv', off_download_root, off_header_root)
    sa = StockAnalysis(os.path.join(on_header_root, on_symbol+'.csv'), 
                       os.path.join(off_header_root, off_symbol+'.csv'),
                       randomState,
                       nb_compute_workers)
    
    exitCode = sa.Compute(compute_root)
    
    if os.path.exists(os.path.join(on_header_root,on_symbol+'.csv')) and os.path.exists(os.path.join(off_header_root,off_symbol+'.csv')):
        if exitCode == 0:
            with open(os.path.join(complete_root, off_symbol), 'w+') as f:
                f.write('0')
            return 0
        elif exitCode == 3:
            with open(os.path.join(complete_root, off_symbol), 'w+') as f:
                f.write('3')
            return 3
        else:
            with open(os.path.join(complete_root, off_symbol), 'w+') as f:
                f.write('2')
            return 2
    else:
        with open(os.path.join(complete_root, off_symbol), 'w+') as f:
            f.write('1')
        return 1

on_head = ""
on_list = []
with open(on_symbol_list, 'r') as f:
    for l in f:
        head, symb = l.strip().split("::")
        if on_head == "":
            on_head = head
        on_list.append(symb)
        
off_head = ""
off_list = []
with open(off_symbol_list, 'r') as f:
    for l in f:
        head, symb = l.strip().split("::")
        if off_head == "":
            off_head = head
        off_list.append(symb)

on_off_union = (set(on_list) & set(off_list))

print("Total number of common stock symbols in on and off list: " + str(len(on_off_union)))

complete_symbol = []
for filename in os.scandir(complete_root):
    if filename.is_file():
        complete_symbol.append(filename.name.split("__")[-1])
        
print("Complete number of common stock symbols in on and off list: " + str(len(complete_symbol)))

on_off_union = on_off_union - set(complete_symbol)

print("Remaining number of common stock symbols in on and off list: " + str(len(complete_symbol)))

on_off_union = list(on_off_union)
on_off_union.sort()

on_off_list = [ (on_head+"::"+symbol, off_head+"::"+symbol) for symbol in on_off_union ]

nb_otq_workers = config['nb_otq_workers']

t0 = time.time()
mte = MultiThreadExecutor(run_stock_analysis_warp)
mte.calculateParallel(on_off_list, nb_otq_workers)
print('time elapsed: ', time.time() - t0)
