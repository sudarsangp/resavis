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
const.REDIS_QUEUE_NAME = "new-metrics-1-1411619234-metrics"
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
		component_metrics = [component_name, avg_execution_time, avg_sum_squares, variance]
		metrics_list.append(component_metrics)

	return render_template("queuemetrics.html", data = metrics_list)

if __name__ == "__main__":
	
	app.run(debug = True)