import redis
from pprint import pprint
import ast
from flask import Flask, render_template

class ComponentBasicInfo:
	component_string = "Component"
	task_string = "Task"
	time_stamp_string = "Timestamp"
	def __init__(self, component, task, time_stamp):
		self.component = component
		self.task = task
		self.time_stamp = time_stamp

	@staticmethod
	def parse_component_basic(parse_data):
		return parse_data.split(':')

class ComponentDetailInfo:
	duration_string = "Duration"
	send_queue_string = "Send Queue"
	recv_queue_string = "Recv Queue"
	execute_string = "Execute"
	def __init__(self, duration, send_queue, recv_queue, execute):
		self.duration = duration
		self.send_queue = send_queue
		self.recv_queue = recv_queue
		self.execute = execute

	@staticmethod
	def parse_component_detail(list_parse_data):
		parse_component_detail = {}
		for key in list_parse_data:
			if str(key) == "duration":
				parse_component_detail["duration"] = list_parse_data[key]
			elif str(key) == "send-queue":
				parse_component_detail["send-queue"] = list_parse_data[key]
			elif str(key) == "recv-queue":
				parse_component_detail["recv-queue"] = list_parse_data[key]
			elif str(key) == "execute":
				parse_component_detail["execute"] = list_parse_data[key]
		return parse_component_detail

class SendQueueDetail:
	read_pos_string = "Read Position"
	write_pos_string = "Write Position"
	capacity_string = "Capacity"
	population_string = "Population"
	def __init__(self, read_pos, write_pos, capacity, population):
		self.read_pos = read_pos
		self.write_pos = write_pos
		self.capacity = capacity
		self.population = population

	@staticmethod
	def parse_send_queue(send_queue):
		parse_send_queue = {}
		for key in send_queue:
			if str(key) == "read_pos":
				parse_send_queue["read_pos"] = send_queue[key]
			elif str(key) == "write_pos":
				parse_send_queue["write_pos"] = send_queue[key]
			elif str(key) == "capacity":
				parse_send_queue["capacity"] = send_queue[key]
			elif str(key) == "population":
				parse_send_queue["population"] = send_queue[key]
		return parse_send_queue

class RecvQueueDetail:
	read_pos_string = "Read Position"
	write_pos_string = "Write Position"
	capacity_string = "Capacity"
	population_string = "Population"
	def __init__(self, read_pos, write_pos, capacity, population):
		self.read_pos = read_pos
		self.write_pos = write_pos
		self.capacity = capacity
		self.population = population

	@staticmethod
	def parse_recv_queue(recv_queue):
		parse_recv_queue = {}
		for key in recv_queue:
			if str(key) == "read_pos":
				parse_recv_queue["read_pos"] = recv_queue[key]
			elif str(key) == "write_pos":
				parse_recv_queue["write_pos"] = recv_queue[key]
			elif str(key) == "capacity":
				parse_recv_queue["capacity"] = recv_queue[key]
			elif str(key) == "population":
				parse_recv_queue["population"] = recv_queue[key]
		return parse_recv_queue

class ExecuteDetail:
	stream_string = "Stream"
	count_string = "Count"
	sum_string =  "Sum"
	sum_squares_string = "Sum of Squares"
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
			parse_execute["stream"] = key
			parse_execute["value"] = execute[key]
		return parse_execute

def connect_to_redis():
	r_server = redis.Redis("localhost")
	result_from_server = r_server.lrange("2fwc-1-1410146732-metrics", 0 , -1)
	return result_from_server

""" write data to file datafromredis which is read from redis server"""
def write_redis_data_to_file():
	file_write = open('datafromredis', 'w')
	file_write.write(str(result_from_server))

def store_redis_data_in_objects(result_from_server):
	tuple_objects = []
	tuple_details = {}
	#print len(result_from_server)
	#pprint(result_from_server)
	for i in range(len(result_from_server)):

		parse_data = result_from_server[i].split('->')
		#print parse_data[0], len(parse_data)
		measure_data = ComponentBasicInfo.parse_component_basic(parse_data[0])
		component_basic = ComponentBasicInfo(measure_data[0], measure_data[1], measure_data[2])
		tuple_details["ComponentBasic"] = component_basic
		#print "Component : %s , Task : %s, TimeStamp : %s" %(measure_data[0], measure_data[1], measure_data[2])
		#print type(parse_data[1])
		list_parse_data = ast.literal_eval(parse_data[1])
		#print type(list_parse_data), list_parse_data

		parse_component_detail = ComponentDetailInfo.parse_component_detail(list_parse_data)
		if "execute" in parse_component_detail:
			component_detail = ComponentDetailInfo(parse_component_detail["duration"], parse_component_detail["send-queue"], parse_component_detail["recv-queue"], parse_component_detail["execute"])
		else:
			component_detail = ComponentDetailInfo(parse_component_detail["duration"], parse_component_detail["send-queue"], parse_component_detail["recv-queue"], None)
		#print componentBasic.component
		tuple_details["ComponentDetail"] = component_detail
		parse_send_queue = SendQueueDetail.parse_send_queue(component_detail.send_queue)
		send_queue = SendQueueDetail(parse_send_queue["read_pos"], parse_send_queue["write_pos"], parse_send_queue["capacity"], parse_send_queue["population"])
		tuple_details["SendQueue"] = send_queue

		parse_recv_queue = RecvQueueDetail.parse_recv_queue(component_detail.recv_queue)
		recv_queue = RecvQueueDetail(parse_recv_queue["read_pos"], parse_recv_queue["write_pos"], parse_recv_queue["capacity"], parse_recv_queue["population"])
		tuple_details["ReceiveQueue"] = recv_queue

		parse_execute = ExecuteDetail.parse_execute(component_detail.execute)
		if parse_execute is not None:
			execute_values = parse_execute["value"].split(',')
			execute = ExecuteDetail(parse_execute["stream"], execute_values[0], execute_values[1], execute_values[2])
			tuple_details["Execute"] = execute
		else:
			tuple_details["Execute"] = None

		tuple_objects.append(tuple_details)
		tuple_details = {}
		# measure_data = None
		# component_basic = None
		# list_parse_data = None
		# parse_component_detail = None
		# component_detail = None
		# parse_send_queue = None
		# send_queue = None
		# parse_recv_queue = None
		# recv_queue = None
		# parse_execute = None
		# execute_values = None
		# execute = None

	print len(tuple_objects)
	""" represents the values stored in member variables"""
	# print component_basic.__dict__
	# print component_detail.__dict__
	# print send_queue.__dict__
	# print recv_queue.__dict__
	# print execute.__dict__
	""" represents the values stored in class variables"""
	#print ExecuteDetail.__dict__
	return tuple_objects

app = Flask(__name__)

@app.route('/')
def index():
	result = connect_to_redis()
	data = store_redis_data_in_objects(result)
	print data[0]["ComponentBasic"].__dict__
	return render_template("index.html", data = data)

if __name__ == "__main__":
	app.run(debug = True)