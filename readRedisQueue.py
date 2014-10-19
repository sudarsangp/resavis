import redis
from pprint import pprint
import ast
from flask import Flask, render_template
import logging
import datetime
import sys
from utilities import const

app = Flask(__name__)

""" constants used in the program that can be assigned only once """
const.KEY_DURATION = "duration"
const.KEY_SEND_QUEUE = "send-queue"
const.KEY_RECV_QUEUE = "recv-queue"
const.KEY_EXECUTE = "execute"

const.KEY_COMPONENT_BASIC = "ComponentBasic"
const.KEY_COMPONENT_DETAIL = "ComponentDetail"
const.KEY_SEND_QUEUE_DETAIL = "SendQueue"
const.KEY_RECV_QUEUE_DETAIL = "ReceiveQueue"

const.KEY_SAMPLE_COUNT = "sampleCount"
const.KEY_TOTAL_QUEUE_LENGTH = "totalQueueLen"
const.KEY_TOTAL_COUNT = "totalCount"
const.KEY_COMPLETE_LATENCY = "complete-latency"

const.KEY_STREAM = "stream"
const.KEY_VALUE = "value"

const.COLON = ':'
const.HIPHEN_GREATHERTHAN = '->'
const.COMMA = ','

const.DEFAULT_LOG_DIRECTORY = "log/"
const.DEFAULT_LOG_FILE_NAME = "timediff.log"
const.DEFAULT_LOGGER_LEVEL = logging.DEBUG
const.DEFAULT_WRITE_FILENAME = "datafromredis"
const.DEFAULT_FILE_MODE = 'w'

const.HOST = "localhost"
const.START_INDEX_FOR_REDIS = 0
const.END_INDEX_FOR_REDIS = -1
const.REDIS_QUEUE_NAME = "19-oct-1-1413701017-metrics"
#old queue id 1410146732
# 2fwc-1-1411573576-metrics

const.HTML_INDEX = "index.html"
const.HTML_ERROR = "error.html"

class ComponentBasicInfo:
	COMPONENT = "Component"
	TASK = "Task"
	TIMESTAMP = "Timestamp"
	def __init__(self, component, task, time_stamp):
		self.component = component
		self.task = task
		self.time_stamp = time_stamp

	@staticmethod
	def parse_component_basic(parse_data):
		return parse_data.split(const.COLON)

class ComponentDetailInfo:
	DURATION = "Duration"
	SEND_QUEUE = "Send Queue"
	RECV_QUEUE = "Recv Queue"
	EXECUTE = "Execute"
	def __init__(self, duration, send_queue, recv_queue, execute):
		self.duration = duration
		self.send_queue = send_queue
		self.recv_queue = recv_queue
		self.execute = execute

	@staticmethod
	def parse_component_detail(list_parse_data):
		parse_component_detail = {}
		for key in list_parse_data:
			if str(key) == const.KEY_DURATION:
				parse_component_detail[const.KEY_DURATION] = list_parse_data[key]
			elif str(key) == const.KEY_SEND_QUEUE:
				parse_component_detail[const.KEY_SEND_QUEUE] = list_parse_data[key]
			elif str(key) == const.KEY_RECV_QUEUE:
				parse_component_detail[const.KEY_RECV_QUEUE] = list_parse_data[key]
			elif str(key) == const.KEY_EXECUTE:
				parse_component_detail[const.KEY_EXECUTE] = list_parse_data[key]
			elif str(key) == const.KEY_COMPLETE_LATENCY:
				parse_component_detail[const.KEY_COMPLETE_LATENCY] = list_parse_data[key]
		return parse_component_detail

class SendQueueDetail:
	SAMPLE_COUNT = "Sample Count"
	DURATION = "Duration"
	TOTAL_QUEUE_LENGTH = "Total Queue Length"
	TOTAL_COUNT = "Total Count"
	def __init__(self, sample_count, duration, total_queue_length, total_count):
		self.sample_count = sample_count
		self.duration = duration
		self.total_queue_length = total_queue_length
		self.total_count = total_count

	@staticmethod
	def parse_send_queue(send_queue):
		if send_queue is None:
			return None
		parse_send_queue = {}
		for key in send_queue:
			if str(key) == const.KEY_SAMPLE_COUNT:
				parse_send_queue[const.KEY_SAMPLE_COUNT] = send_queue[key]
			elif str(key) == const.KEY_DURATION:
				parse_send_queue[const.KEY_DURATION] = send_queue[key]
			elif str(key) == const.KEY_TOTAL_QUEUE_LENGTH:
				parse_send_queue[const.KEY_TOTAL_QUEUE_LENGTH] = send_queue[key]
			elif str(key) == const.KEY_TOTAL_COUNT:
				parse_send_queue[const.KEY_TOTAL_COUNT] = send_queue[key]
		return parse_send_queue

class RecvQueueDetail:
	SAMPLE_COUNT = "Sample Count"
	DURATION = "Duration"
	TOTAL_QUEUE_LENGTH = "Total Queue Length"
	TOTAL_COUNT = "Total Count"
	def __init__(self, sample_count, duration, total_queue_length, total_count):
		self.sample_count = sample_count
		self.duration = duration
		self.total_queue_length = total_queue_length
		self.total_count = total_count

	@staticmethod
	def parse_recv_queue(recv_queue):
		if recv_queue is None:
			return None
		parse_recv_queue = {}
		for key in recv_queue:
			if str(key) == const.KEY_SAMPLE_COUNT:
				parse_recv_queue[const.KEY_SAMPLE_COUNT] = recv_queue[key]
			elif str(key) == const.KEY_DURATION:
				parse_recv_queue[const.KEY_DURATION] = recv_queue[key]
			elif str(key) == const.KEY_TOTAL_QUEUE_LENGTH:
				parse_recv_queue[const.KEY_TOTAL_QUEUE_LENGTH] = recv_queue[key]
			elif str(key) == const.KEY_TOTAL_COUNT:
				parse_recv_queue[const.KEY_TOTAL_COUNT] = recv_queue[key]
		return parse_recv_queue

class ExecuteDetail:
	STREAM = "Stream"
	COUNT = "Count"
	SUM =  "Sum"
	SUM_OF_SQUARES = "Sum of Squares"
	def __init__(self, stream, count, sum_value, sum_squares):
		self.stream = stream
		self.count = count
		self.sum_value = sum_value
		self.sum_squares = sum_squares

	@staticmethod
	def parse_execute(execute):
		if execute is None:
			return None
		parse_execute = {}
		for key in execute:
			parse_execute[const.KEY_STREAM] = key
			parse_execute[const.KEY_VALUE] = execute[key]
		return parse_execute

def initialize_logger(filename=const.DEFAULT_LOG_FILE_NAME, logger_level = const.DEFAULT_LOGGER_LEVEL):
	logging.basicConfig(filename = const.DEFAULT_LOG_DIRECTORY + filename, level = logger_level)

def connect_to_redis(host, start_index, stop_index, redis_queue_name):
	try:
		start = datetime.datetime.now()
		r_server = redis.Redis(host)
		result_from_server = r_server.lrange(redis_queue_name, start_index , stop_index)
		end = datetime.datetime.now()
		time_diff = end - start
		data_size = len(result_from_server)
		logging.debug('Time to connect to redis on %s and read data of size %s is %s' %(host, str(data_size), str(time_diff)))
		return result_from_server
	except:
		print "Redis Server not online",sys.exc_info()[0]

""" write data to file datafromredis which is read from redis server"""
def write_redis_data_to_file(filename = const.DEFAULT_WRITE_FILENAME, file_mode = const.DEFAULT_FILE_MODE):
	file_write = open(filename, file_mode)
	file_write.write(str(result_from_server))

def get_component_details_values_for_keys(parse_component_detail):
	duration_component_details = None
	send_queue_component_details = None
	recv_queue_component_details = None
	execute_component_details = None
	
	if const.KEY_DURATION in parse_component_detail:
		duration_component_details = parse_component_detail[const.KEY_DURATION]
	if const.KEY_SEND_QUEUE in parse_component_detail:
		send_queue_component_details = parse_component_detail[const.KEY_SEND_QUEUE]
	if const.KEY_RECV_QUEUE in parse_component_detail:
		recv_queue_component_details = parse_component_detail[const.KEY_RECV_QUEUE]
	if const.KEY_EXECUTE in parse_component_detail:
		execute_component_details = parse_component_detail[const.KEY_EXECUTE]
	elif const.KEY_COMPLETE_LATENCY in parse_component_detail:
		execute_component_details = parse_component_detail[const.KEY_COMPLETE_LATENCY]

	return duration_component_details, send_queue_component_details, recv_queue_component_details, execute_component_details

def store_redis_data_in_objects(result_from_server):
	start = datetime.datetime.now()
	tuple_objects = []
	tuple_details = {}
	for i in range(len(result_from_server)):
		
		parse_data = result_from_server[i].split(const.HIPHEN_GREATHERTHAN)
		measure_data = ComponentBasicInfo.parse_component_basic(parse_data[0])
		component_basic = ComponentBasicInfo(measure_data[0], measure_data[1], measure_data[2])
		tuple_details[const.KEY_COMPONENT_BASIC] = component_basic
		list_parse_data = ast.literal_eval(parse_data[1])

		parse_component_detail = ComponentDetailInfo.parse_component_detail(list_parse_data)
		duration_component_details, send_queue_component_details, recv_queue_component_details, execute_component_details = get_component_details_values_for_keys(parse_component_detail)
		component_detail = ComponentDetailInfo(duration_component_details, send_queue_component_details, recv_queue_component_details, execute_component_details)
		
		tuple_details[const.KEY_COMPONENT_DETAIL] = component_detail
		parse_send_queue = SendQueueDetail.parse_send_queue(component_detail.send_queue)
		if parse_send_queue is not None:
			send_queue = SendQueueDetail(parse_send_queue[const.KEY_SAMPLE_COUNT], parse_send_queue[const.KEY_DURATION], parse_send_queue[const.KEY_TOTAL_QUEUE_LENGTH], parse_send_queue[const.KEY_TOTAL_COUNT])
			tuple_details[const.KEY_SEND_QUEUE_DETAIL] = send_queue
		else:
			tuple_details[const.KEY_SEND_QUEUE_DETAIL] = None

		parse_recv_queue = RecvQueueDetail.parse_recv_queue(component_detail.recv_queue)
		if parse_recv_queue is not None:
			recv_queue = RecvQueueDetail(parse_recv_queue[const.KEY_SAMPLE_COUNT], parse_recv_queue[const.KEY_DURATION], parse_recv_queue[const.KEY_TOTAL_QUEUE_LENGTH], parse_recv_queue[const.KEY_TOTAL_COUNT])
			tuple_details[const.KEY_RECV_QUEUE_DETAIL] = recv_queue
		else:
			tuple_details[const.KEY_RECV_QUEUE_DETAIL] = None

		parse_execute = ExecuteDetail.parse_execute(component_detail.execute)
		if parse_execute is not None:
			execute_values = parse_execute[const.KEY_VALUE].split(const.COMMA)
			execute = ExecuteDetail(parse_execute[const.KEY_STREAM], execute_values[0], execute_values[1], execute_values[2])
			tuple_details[ComponentDetailInfo.EXECUTE] = execute
		else:
			tuple_details[ComponentDetailInfo.EXECUTE] = None

		tuple_objects.append(tuple_details)
		tuple_details = {}

	end = datetime.datetime.now()
	time_diff = end - start
	data_size = len(result_from_server)
	logging.debug('Time to process data of size %s is %s' %(str(data_size), str(time_diff)))

	print len(tuple_objects)
	""" represents the values stored in member variables"""
	# print component_basic.__dict__
	""" represents the values stored in class variables"""
	#print ExecuteDetail.__dict__
	return tuple_objects

def initial_setup():
	initialize_logger()
	global result, data
	result = connect_to_redis(const.HOST, const.START_INDEX_FOR_REDIS, const.END_INDEX_FOR_REDIS , const.REDIS_QUEUE_NAME)
	if result is not None:
		data = store_redis_data_in_objects(result)
	return result,data

@app.route('/')
def index():
	result, data = initial_setup()
	if result is None:
		return render_template(const.HTML_ERROR)

	#print data[0][const.KEY_COMPONENT_BASIC].__dict__
	return render_template(const.HTML_INDEX, data = data)

""" TODO: get the name of the spouts and bolts from a different file to avoid hardcoding """
@app.route('/spoutbolt')
def spout_details():
	result, data = initial_setup()
	if result is None:
		return render_template(const.HTML_ERROR)
	
	spout_data = []
	bolt_one = []
	bolt_two = []
	for i in range(len(data)):
		if 'sentenceSpout' in str(data[i][const.KEY_COMPONENT_BASIC].component):
			spout_data.append(data[i][const.KEY_COMPONENT_BASIC])
		elif 'split' in str(data[i][const.KEY_COMPONENT_BASIC].component):
			bolt_one.append(data[i][const.KEY_COMPONENT_BASIC])
		elif 'counter' in str(data[i][const.KEY_COMPONENT_BASIC].component):
			bolt_two.append(data[i][const.KEY_COMPONENT_BASIC])
	spout_bolt_data = [spout_data, bolt_one, bolt_two]
	return render_template("spoutbolt.html", data = spout_bolt_data)

""" TODO: get the name of the spouts and bolts from a different file to avoid hardcoding """
@app.route('/queuemetrics')
def queue_metrics():
	result, data = initial_setup()
	if result is None:
		return render_template(const.HTML_ERROR)

	avg_execution_time = 0
	avg_sum_squares = 0
	variance = 0
	component_metrics = []
	metrics_list = []
	for i in range(len(data)):
		task_metric = data[i][ComponentDetailInfo.EXECUTE]
		if task_metric is None:
			continue
		avg_execution_time = float(task_metric.sum_value) / float(task_metric.count)
		avg_sum_squares = float(task_metric.sum_squares) / float(task_metric.count)
		variance = avg_sum_squares - (avg_execution_time * avg_execution_time)
		component_name = str(data[i][const.KEY_COMPONENT_BASIC].component)
		task_id = str(data[i][const.KEY_COMPONENT_BASIC].task)
		component_metrics = [component_name, task_id, avg_execution_time, avg_sum_squares, variance]
		metrics_list.append(component_metrics)

	return render_template("queuemetrics.html", data = metrics_list)

@app.route('/compoenetlevel')
def compoenent_level():
	result, data = initial_setup()
	if result is None:
		return render_template(const.HTML_ERROR)

	timestamp_values_3 = []
	timestamp_values_4 = []
	timestamp_values_5 = []
	timestamp_values_6 = []
	timestamp_values_7 = []
	timestamp_values_8 = []
	timestamp_values_9 = []
	timestamp_values_10 = []
	timestamp_values_11 = []
	timestamp_values_12 = []
	timestamp_values_13 = []
	timestamp_values_14 = []
	timestamp_values_15 = []
	timestamp_values_16 = []
	timestamp_values_17 = []
	timestamp_values_18 = []
	timestamp_values_19 = []
	timestamp_values_20 = []
	timestamp_values_21 = []
	timestamp_values_22 = []
	timestamp_values_23 = []

	timestamp_dict = {}

	sum_array = []
	sum_sq_array = []
	count_array = []
	avg_execution_time = []
	avg_sum_squares = []
	variance = []
	for i in range(21):
		sum_array.append(0)
		sum_sq_array.append(0)
		count_array.append(0)
		avg_execution_time.append(0)
		avg_sum_squares.append(0)
		variance.append(0)

	for i in range(len(data)):
		# executing is becoming none for most tuples but need that for average calculation
		# task_metric = data[i][ComponentDetailInfo.EXECUTE]
		# if task_metric is None:
		# 	continue

		if(int(data[i][const.KEY_COMPONENT_BASIC].task) == 3):
			timestamp_values_3.append(data[i][const.KEY_COMPONENT_BASIC].time_stamp)
			component_task_key = data[i][const.KEY_COMPONENT_BASIC].component + str(data[i][const.KEY_COMPONENT_BASIC].task)
			timestamp_dict[component_task_key] = timestamp_values_3
			task_metric = data[i][ComponentDetailInfo.EXECUTE]
			if task_metric is None:
			 	continue
			sum_array[0] += float(task_metric.sum_value)
			sum_sq_array[0] += float(task_metric.sum_squares)
			count_array[0] += float(task_metric.count)

		elif(int(data[i][const.KEY_COMPONENT_BASIC].task) == 4):
			timestamp_values_4.append(data[i][const.KEY_COMPONENT_BASIC].time_stamp)
			component_task_key = data[i][const.KEY_COMPONENT_BASIC].component + str(data[i][const.KEY_COMPONENT_BASIC].task)
			timestamp_dict[component_task_key] = timestamp_values_4
			task_metric = data[i][ComponentDetailInfo.EXECUTE]
			if task_metric is None:
			 	continue
			sum_array[1] += float(task_metric.sum_value)
			sum_sq_array[1] += float(task_metric.sum_squares)
			count_array[1] += float(task_metric.count)

		elif(int(data[i][const.KEY_COMPONENT_BASIC].task) == 5):
			timestamp_values_5.append(data[i][const.KEY_COMPONENT_BASIC].time_stamp)
			component_task_key = data[i][const.KEY_COMPONENT_BASIC].component + str(data[i][const.KEY_COMPONENT_BASIC].task)
			timestamp_dict[component_task_key] = timestamp_values_5
			task_metric = data[i][ComponentDetailInfo.EXECUTE]
			if task_metric is None:
			 	continue
			sum_array[2] += float(task_metric.sum_value)
			sum_sq_array[2] += float(task_metric.sum_squares)
			count_array[2] += float(task_metric.count)

		elif(int(data[i][const.KEY_COMPONENT_BASIC].task) == 6):
			timestamp_values_6.append(data[i][const.KEY_COMPONENT_BASIC].time_stamp)
			component_task_key = data[i][const.KEY_COMPONENT_BASIC].component + str(data[i][const.KEY_COMPONENT_BASIC].task)
			timestamp_dict[component_task_key] = timestamp_values_6
			task_metric = data[i][ComponentDetailInfo.EXECUTE]
			if task_metric is None:
			 	continue
			sum_array[3] += float(task_metric.sum_value)
			sum_sq_array[3] += float(task_metric.sum_squares)
			count_array[3] += float(task_metric.count)

		elif(int(data[i][const.KEY_COMPONENT_BASIC].task) == 7):
			timestamp_values_7.append(data[i][const.KEY_COMPONENT_BASIC].time_stamp)
			component_task_key = data[i][const.KEY_COMPONENT_BASIC].component + str(data[i][const.KEY_COMPONENT_BASIC].task)
			timestamp_dict[component_task_key] = timestamp_values_7
			task_metric = data[i][ComponentDetailInfo.EXECUTE]
			if task_metric is None:
			 	continue
			sum_array[4] += float(task_metric.sum_value)
			sum_sq_array[4] += float(task_metric.sum_squares)
			count_array[4] += float(task_metric.count)

		elif(int(data[i][const.KEY_COMPONENT_BASIC].task) == 8):
			timestamp_values_8.append(data[i][const.KEY_COMPONENT_BASIC].time_stamp)
			component_task_key = data[i][const.KEY_COMPONENT_BASIC].component + str(data[i][const.KEY_COMPONENT_BASIC].task)
			timestamp_dict[component_task_key] = timestamp_values_8
			task_metric = data[i][ComponentDetailInfo.EXECUTE]
			if task_metric is None:
			 	continue
			sum_array[5] += float(task_metric.sum_value)
			sum_sq_array[5] += float(task_metric.sum_squares)
			count_array[5] += float(task_metric.count)

		elif(int(data[i][const.KEY_COMPONENT_BASIC].task) == 9):
			timestamp_values_9.append(data[i][const.KEY_COMPONENT_BASIC].time_stamp)
			component_task_key = data[i][const.KEY_COMPONENT_BASIC].component + str(data[i][const.KEY_COMPONENT_BASIC].task)
			timestamp_dict[component_task_key] = timestamp_values_9
			task_metric = data[i][ComponentDetailInfo.EXECUTE]
			if task_metric is None:
			 	continue
			sum_array[6] += float(task_metric.sum_value)
			sum_sq_array[6] += float(task_metric.sum_squares)
			count_array[6] += float(task_metric.count)

		elif(int(data[i][const.KEY_COMPONENT_BASIC].task) == 10):
			timestamp_values_10.append(data[i][const.KEY_COMPONENT_BASIC].time_stamp)
			component_task_key = data[i][const.KEY_COMPONENT_BASIC].component + str(data[i][const.KEY_COMPONENT_BASIC].task)
			timestamp_dict[component_task_key] = timestamp_values_10
			task_metric = data[i][ComponentDetailInfo.EXECUTE]
			if task_metric is None:
			 	continue
			sum_array[7] += float(task_metric.sum_value)
			sum_sq_array[7] += float(task_metric.sum_squares)
			count_array[7] += float(task_metric.count)

		elif(int(data[i][const.KEY_COMPONENT_BASIC].task) == 11):
			timestamp_values_11.append(data[i][const.KEY_COMPONENT_BASIC].time_stamp)
			component_task_key = data[i][const.KEY_COMPONENT_BASIC].component + str(data[i][const.KEY_COMPONENT_BASIC].task)
			timestamp_dict[component_task_key] = timestamp_values_11
			task_metric = data[i][ComponentDetailInfo.EXECUTE]
			if task_metric is None:
			 	continue
			sum_array[8] += float(task_metric.sum_value)
			sum_sq_array[8] += float(task_metric.sum_squares)
			count_array[8] += float(task_metric.count)

		elif(int(data[i][const.KEY_COMPONENT_BASIC].task) == 12):
			timestamp_values_12.append(data[i][const.KEY_COMPONENT_BASIC].time_stamp)
			component_task_key = data[i][const.KEY_COMPONENT_BASIC].component + str(data[i][const.KEY_COMPONENT_BASIC].task)
			timestamp_dict[component_task_key] = timestamp_values_12
			task_metric = data[i][ComponentDetailInfo.EXECUTE]
			if task_metric is None:
			 	continue
			sum_array[9] += float(task_metric.sum_value)
			sum_sq_array[9] += float(task_metric.sum_squares)
			count_array[9] += float(task_metric.count)

		elif(int(data[i][const.KEY_COMPONENT_BASIC].task) == 13):
			timestamp_values_13.append(data[i][const.KEY_COMPONENT_BASIC].time_stamp)
			component_task_key = data[i][const.KEY_COMPONENT_BASIC].component + str(data[i][const.KEY_COMPONENT_BASIC].task)
			timestamp_dict[component_task_key] = timestamp_values_13
			task_metric = data[i][ComponentDetailInfo.EXECUTE]
			if task_metric is None:
			 	continue
			sum_array[10] += float(task_metric.sum_value)
			sum_sq_array[10] += float(task_metric.sum_squares)
			count_array[10] += float(task_metric.count)

		elif(int(data[i][const.KEY_COMPONENT_BASIC].task) == 14):
			timestamp_values_14.append(data[i][const.KEY_COMPONENT_BASIC].time_stamp)
			component_task_key = data[i][const.KEY_COMPONENT_BASIC].component + str(data[i][const.KEY_COMPONENT_BASIC].task)
			timestamp_dict[component_task_key] = timestamp_values_14
			task_metric = data[i][ComponentDetailInfo.EXECUTE]
			if task_metric is None:
			 	continue
			sum_array[11] += float(task_metric.sum_value)
			sum_sq_array[11] += float(task_metric.sum_squares)
			count_array[11] += float(task_metric.count)

		elif(int(data[i][const.KEY_COMPONENT_BASIC].task) == 15):
			timestamp_values_15.append(data[i][const.KEY_COMPONENT_BASIC].time_stamp)
			component_task_key = data[i][const.KEY_COMPONENT_BASIC].component + str(data[i][const.KEY_COMPONENT_BASIC].task)
			timestamp_dict[component_task_key] = timestamp_values_15
			task_metric = data[i][ComponentDetailInfo.EXECUTE]
			if task_metric is None:
			 	continue
			sum_array[12] += float(task_metric.sum_value)
			sum_sq_array[12] += float(task_metric.sum_squares)
			count_array[12] += float(task_metric.count)

		elif(int(data[i][const.KEY_COMPONENT_BASIC].task) == 16):
			timestamp_values_16.append(data[i][const.KEY_COMPONENT_BASIC].time_stamp)
			component_task_key = data[i][const.KEY_COMPONENT_BASIC].component + str(data[i][const.KEY_COMPONENT_BASIC].task)
			timestamp_dict[component_task_key] = timestamp_values_16
			task_metric = data[i][ComponentDetailInfo.EXECUTE]
			if task_metric is None:
			 	continue
			sum_array[13] += float(task_metric.sum_value)
			sum_sq_array[13] += float(task_metric.sum_squares)
			count_array[13] += float(task_metric.count)

		elif(int(data[i][const.KEY_COMPONENT_BASIC].task) == 17):
			timestamp_values_17.append(data[i][const.KEY_COMPONENT_BASIC].time_stamp)
			component_task_key = data[i][const.KEY_COMPONENT_BASIC].component + str(data[i][const.KEY_COMPONENT_BASIC].task)
			timestamp_dict[component_task_key] = timestamp_values_17
			task_metric = data[i][ComponentDetailInfo.EXECUTE]
			if task_metric is None:
			 	continue
			sum_array[14] += float(task_metric.sum_value)
			sum_sq_array[14] += float(task_metric.sum_squares)
			count_array[14] += float(task_metric.count)

		elif(int(data[i][const.KEY_COMPONENT_BASIC].task) == 18):
			timestamp_values_18.append(data[i][const.KEY_COMPONENT_BASIC].time_stamp)
			component_task_key = data[i][const.KEY_COMPONENT_BASIC].component + str(data[i][const.KEY_COMPONENT_BASIC].task)
			timestamp_dict[component_task_key] = timestamp_values_18
			task_metric = data[i][ComponentDetailInfo.EXECUTE]
			if task_metric is None:
			 	continue
			sum_array[15] += float(task_metric.sum_value)
			sum_sq_array[15] += float(task_metric.sum_squares)
			count_array[15] += float(task_metric.count)

		elif(int(data[i][const.KEY_COMPONENT_BASIC].task) == 19):
			timestamp_values_19.append(data[i][const.KEY_COMPONENT_BASIC].time_stamp)
			component_task_key = data[i][const.KEY_COMPONENT_BASIC].component + str(data[i][const.KEY_COMPONENT_BASIC].task)
			timestamp_dict[component_task_key] = timestamp_values_19
			task_metric = data[i][ComponentDetailInfo.EXECUTE]
			if task_metric is None:
			 	continue
			sum_array[16] += float(task_metric.sum_value)
			sum_sq_array[16] += float(task_metric.sum_squares)
			count_array[16] += float(task_metric.count)

		elif(int(data[i][const.KEY_COMPONENT_BASIC].task) == 20):
			timestamp_values_20.append(data[i][const.KEY_COMPONENT_BASIC].time_stamp)
			component_task_key = data[i][const.KEY_COMPONENT_BASIC].component + str(data[i][const.KEY_COMPONENT_BASIC].task)
			timestamp_dict[component_task_key] = timestamp_values_20
			task_metric = data[i][ComponentDetailInfo.EXECUTE]
			if task_metric is None:
			 	continue
			sum_array[17] += float(task_metric.sum_value)
			sum_sq_array[17] += float(task_metric.sum_squares)
			count_array[17] += float(task_metric.count)

		elif(int(data[i][const.KEY_COMPONENT_BASIC].task) == 21):
			timestamp_values_21.append(data[i][const.KEY_COMPONENT_BASIC].time_stamp)
			component_task_key = data[i][const.KEY_COMPONENT_BASIC].component + str(data[i][const.KEY_COMPONENT_BASIC].task)
			timestamp_dict[component_task_key] = timestamp_values_21
			task_metric = data[i][ComponentDetailInfo.EXECUTE]
			if task_metric is None:
			 	continue
			sum_array[18] += float(task_metric.sum_value)
			sum_sq_array[18] += float(task_metric.sum_squares)
			count_array[18] += float(task_metric.count)

		elif(int(data[i][const.KEY_COMPONENT_BASIC].task) == 22):
			timestamp_values_22.append(data[i][const.KEY_COMPONENT_BASIC].time_stamp)
			component_task_key = data[i][const.KEY_COMPONENT_BASIC].component + str(data[i][const.KEY_COMPONENT_BASIC].task)
			timestamp_dict[component_task_key] = timestamp_values_22
			task_metric = data[i][ComponentDetailInfo.EXECUTE]
			if task_metric is None:
			 	continue
			sum_array[19] += float(task_metric.sum_value)
			sum_sq_array[19] += float(task_metric.sum_squares)
			count_array[19] += float(task_metric.count)

		elif(int(data[i][const.KEY_COMPONENT_BASIC].task) == 23):
			timestamp_values_23.append(data[i][const.KEY_COMPONENT_BASIC].time_stamp)
			component_task_key = data[i][const.KEY_COMPONENT_BASIC].component + str(data[i][const.KEY_COMPONENT_BASIC].task)
			timestamp_dict[component_task_key] = timestamp_values_23
			task_metric = data[i][ComponentDetailInfo.EXECUTE]
			if task_metric is None:
			 	continue
			sum_array[20] += float(task_metric.sum_value)
			sum_sq_array[20] += float(task_metric.sum_squares)
			count_array[20] += float(task_metric.count)

	
	for i in range(21):
		if(count_array[i]!=0):
			avg_execution_time[i] = sum_array[i] / count_array[i]
			avg_sum_squares[i] = sum_sq_array[i] / count_array[i]
			variance[i] = avg_sum_squares[i] - (avg_execution_time[i] * avg_execution_time[i])

	print avg_execution_time
	print avg_sum_squares
	print variance
		# component_task_key = data[i][const.KEY_COMPONENT_BASIC].component + str(data[i][const.KEY_COMPONENT_BASIC].task)
		# timestamp_key = str(data[i][const.KEY_COMPONENT_BASIC].task) + '-' + str(data[i][const.KEY_COMPONENT_BASIC].time_stamp)
		# timestamp_values.push(data[i][const.KEY_COMPONENT_BASIC].time_stamp)
		# timestamp_dict[timestamp_key] = timestamp_values
	return render_template("componentaggregate.html", data = timestamp_dict, avg_execution_time = avg_execution_time, avg_sum_squares = avg_sum_squares, variance = variance)

@app.route('/executorlevel')
def executor_level():
	result, data = initial_setup()
	if result is None:
		return render_template(const.HTML_ERROR)

	split_executors = []
	combine_tasks_split = []
	sum_duration_split = 0

	counter_executors = []
	combine_tasks_counter = []
	sum_duration_counter = 0

	for i in range(len(data)):
		if 'sentenceSpout' in str(data[i][const.KEY_COMPONENT_BASIC].component):
			pass

		elif 'split' in str(data[i][const.KEY_COMPONENT_BASIC].component):
			sum_duration_split += data[i][const.KEY_SEND_QUEUE_DETAIL].duration
			if sum_duration_split < 70000:
				combine_tasks_split.append(data[i][const.KEY_SEND_QUEUE_DETAIL])
			else:
				split_executors.append(combine_tasks_split)
				combine_tasks_split = []
				sum_duration_split = 0

		elif 'counter' in str(data[i][const.KEY_COMPONENT_BASIC].component):
			sum_duration_counter += data[i][const.KEY_SEND_QUEUE_DETAIL].duration
			if sum_duration_counter < 70000:
				combine_tasks_counter.append(data[i][const.KEY_SEND_QUEUE_DETAIL])
			else:
				counter_executors.append(combine_tasks_counter)
				combine_tasks_counter = []
				sum_duration_counter = 0

	print len(split_executors), len(counter_executors)

	sum_duration = 0
	sum_total_count = 0
	avg_rate = 0.0

	sum_total_queue_length = 0
	sum_sample_count = 0
	avg_queue_length = 0

	split_avg_rates = []
	split_avg_queue_length = []

	for i in range(len(split_executors)):
		split_avg_rates.append(0)
		split_avg_queue_length.append(0)
	
	for i in range(len(split_executors)):
		
		for j in range(len(split_executors[i])):
			sum_duration+=split_executors[i][j].duration
			sum_total_count+=split_executors[i][j].total_count
			sum_total_queue_length += split_executors[i][j].total_queue_length
			sum_sample_count += split_executors[i][j].sample_count
		
		if sum_duration > 0:
			avg_rate = float(sum_total_count)/float(sum_duration)
			split_avg_rates[i] = avg_rate
		if sum_sample_count > 0:
			avg_queue_length = float(sum_total_queue_length)/float(sum_sample_count)
			split_avg_queue_length[i] = avg_queue_length

	sum_duration = 0
	sum_total_count = 0
	avg_rate = 0.0

	sum_total_queue_length = 0
	sum_sample_count = 0
	avg_queue_length = 0

	counter_avg_rates = []
	counter_avg_queue_length = []

	for i in range(len(counter_executors)):
		counter_avg_rates.append(0)
		counter_avg_queue_length.append(0)
	
	for i in range(len(counter_executors)):
		
		for j in range(len(counter_executors[i])):
			sum_duration+=counter_executors[i][j].duration
			sum_total_count+=counter_executors[i][j].total_count
			sum_total_queue_length += counter_executors[i][j].total_queue_length
			sum_sample_count += counter_executors[i][j].sample_count
		
		if sum_duration > 0:
			avg_rate = float(sum_total_count)/ float(sum_duration)
			counter_avg_rates[i] = avg_rate
		if sum_sample_count > 0:
			avg_queue_length = float(sum_total_queue_length)/float(sum_sample_count)
			counter_avg_queue_length[i] = avg_queue_length

	split_details = [split_avg_rates, split_avg_queue_length]
	counter_details = [counter_avg_rates, counter_avg_queue_length]

	return render_template("executoraggregate.html", split_details = split_details, counter_details = counter_details)
if __name__ == "__main__":
	
	app.run(debug = True)