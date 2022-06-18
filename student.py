##
## note: 
## (1) transaction_log (done)
## (2) API post (done)


import pandas as pd 
import pymongo, random, json, requests, logging
from pymongo import monitoring
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.responses import JSONResponse


class CommandLogger(monitoring.CommandListener):
	""" log mongo queries """
	def started(self, event):
		msg = "Command ST {0.command_name} **Details** {0.command} ".format(event)
		print ("\n",msg)
		logging.info(msg)

	def succeeded(self, event):
		msg = "Succeded command {0.command_name} id {0.request_id} completed in {0.duration_micros} microseconds".format(event)
		print ("\n",msg)
		logging.info(msg)

	def failed(self, event):
		msg = "Failed command {0.command_name} id {0.request_id} completed in {0.duration_micros} microseconds".format(event)
		print ("\n",msg)
		logging.info(msg)

class Student():
	""" New student properties """

	def __init__(self):
		self.first_name = None
		self.last_name = None
		self.dob = None
		self.student_id = None
		self.phone_number = None
		self.school_name = None
		self.transfer_date = None
		self.classes = []
		self.notes = []

	def prepare_student_info(self, row):
		""" CSV: extract student info from csv other than classes """
		if (row[0] == 'first_name'): self.first_name = row[1].strip().title() + '_' + str(random.randint(100,250))
		elif (row[0] == 'last_name'): self.last_name = row[1].strip().title() + '_' + str(random.randint(100,250))
		elif (row[0] == 'dob'): self.dob = row[1].strip().title()
		elif (row[0] == 'phone_number'): self.phone_number = row[1].strip().title()
		elif (row[0] == 'school_name'): self.school_name = row[1].strip().title()
		elif (row[0] == 'transfer_date'): self.transfer_date = row[1].strip().title()

	def prepare_student_classes(self, row):
		""" CSV: extract student classes from csv """
		temp = dict()
		temp['class'] = row[0]
		temp['grade'] = row[1]
		temp['term'] = row[2]
		temp['date'] = row[3]
		
		self.classes.append(temp)

	def load_transcript_csv(self, records):
		""" CSV: load student transcript (student info & student classes) to student object """

		for i in range(len(records)):
			current_record = records.iloc[i]

			# pass in student info
			if (i < 7): self.prepare_student_info(current_record)

			# pass in student classes
			elif (i >= 9): self.prepare_student_classes(current_record)

	def load_transcript_direct(self, records):
		""" load transcript from JSON to student object """

		self.first_name = records['first_name']
		self.last_name = records['last_name']
		self.dob = records['dob']
		self.student_id = records['student_id']
		self.phone_number = records['phone_number']
		self.school_name = records['school_name']
		self.transfer_date = records['transfer_date']
		self.classes = records['classes']

	def import_transcript_db(self):
		""" import data to db """

		# check for error, missing critical fields
		record = self.check_transcript()

		# Abort operation when critical error = 0
		if (self.notes and min(self.notes) == 0): return (self.notes)
		else:

			# check if student exists
			if (student_exist_by_name_dob(self.first_name, self.last_name, self.dob) > 0):
				error_msg = f"Student {self.first_name, self.last_name} already exists. No import."
				logging.info(error_msg)
				print (error_msg)
				return (error_msg)

			else:				
				# student_info
				print (f"Importing student {self.first_name, self.last_name} info to student db")
				next_student_id = get_next_student_id()

				# assign student id number
				record['student_id'] = next_student_id
				record_student_info = {key:val for key,val in record.items() if key != 'classes'}

				record_student_info['student_id'] = next_student_id	# assign student_id
				student_info_db.insert_one(record_student_info)

				# class_info
				if (not record['classes']): print ("No classes to import.")
				else:
					print ("Importing student classes to classes db")
					for one_class in record['classes']:
						this_class = {ind:val for ind, val in one_class.items()}
						this_class['student_id'] = next_student_id

						#print (this_class)
						if (check_student_class_exist(next_student_id, this_class['class'], this_class['term']) > 0):
							print ("[Error] This class exists. Not importing.")
						else:
							student_grade_db.insert_one(this_class)

				return record

	def check_transcript(self):
		""" check transcript for missing detail """

		record = {}

		# encode into dictionary format & check for errors #
		record['first_name'] = self.first_name
		record['last_name'] = self.last_name
		record['dob'] = self.dob
		record['student_id'] = self.student_id
		record['phone_number'] = self.phone_number
		record['school_name'] = self.school_name
		record['transfer_date'] = self.transfer_date
		record['classes'] = self.classes

		# check for missing info.
		for key,val in record.items():
			if (not val and key not in ('classes','transfer_date','phone_number','school_name','student_id')):
				# critical error
				error_msg = f"[red flag] Error. '{key}' is required."
				logging.info(error_msg)
				print (error_msg)
				self.notes.append({0:error_msg})

			elif (not val):
				# minor error
				self.notes.append({5:f"[info] {key} is missing data."})

		return (record)



def edit_student_info(field_name,edit_info,student_id):
	""" edit / update student info """

	query = { "student_id": student_id }
	update_value = { "$set": { field_name: edit_info } }
	student_info_db.update_one(query, update_value)

	#print "customers" after the update:
	for x in student_info_db.find(query,{'first_name':1,'last_name':1,'dob':1,'student_id':1, '_id':0}):
		return (x)

def student_exist_by_id(student_id):
	""" check student db to see if student exists by student_id """
	""" Return True if exists """

	return True if student_info_db.count_documents({"student_id":student_id}) > 0 else False

def student_exist_by_name_dob(first_name, last_name, dob):
	""" check student by first_name, last_name, and dob 
	return True if student exists """

	my_query = {"first_name":first_name,"last_name":last_name,"dob":dob}
	return student_info_db.count_documents(my_query)

def get_next_student_id():
	""" return latest student_id + 1 """
	if (student_info_db.count_documents({}) == 0): return 1000
	else:
		last_id = student_info_db.find({},{'student_id':1}).sort([['student_id',pymongo.DESCENDING]]).limit(1)
		return ([row['student_id'] for row in last_id][0] + 1)

def check_student_class_exist(student_id,class_name,class_term):
	""" check for class by class_name, class_term
	return True if class exists """

	my_query = {"student_id":student_id,"class":class_name,"term":class_term}
	#self.notes.append({5:f"[info] {class_name} exists. Duplicate."})
	return student_grade_db.count_documents(my_query)

#print (student_grade_db.delete_many({'student_id':1027}).deleted_count)


""" start mongodb loggings & other logic errors & save to log.txt """
logging.basicConfig(filename='log.txt', filemode='a', format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s', datefmt='%H:%M:%S', level=logging.DEBUG)
logging.info("Logging Student mongodb queries & logic errors")
monitoring.register(CommandLogger())

""" mongodb """
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["students_transfer"]
student_info_db = mydb["students_info"]
student_grade_db = mydb["students_grade"]
#student_info_db.delete_many({})

""" Fast API """
app = FastAPI()

@app.get("/",response_class=HTMLResponse)
async def default():
	""" default view """
	return "import: <a href='./importcsv_one'>csv - one </a>, <a href='./importcsv_multiple'>csv - multiple</a>, <a href='./importjson'>api</a>  | view: <a href='./view/?display=50&page=0'>all</a> <a href='./view_id/1000'>by id</a> | <a href='./remove/1000'>remove</a>"

@app.get("/edit/")
def edit_student(rservice,rtype,update_info,student_id):
	""" edit student info ; update_info in JSON format """
	#http://127.0.0.1:8000/edit/?rservice=student_info&rtype=edit&update_info={"first_name":"Love","last_name":"Birds"}&student_id=2450
	
	student_id = int(student_id)

	# catch improper format
	try:
		updates = (json.loads(update_info.replace("'",'"')))
		res = ""
		if (rservice == 'student_info' and rtype == 'edit' and student_id > 0):
			if (student_exist_by_id(student_id)):
				for field_name, edit_info in updates.items():
					if ( edit_info and field_name in ('first_name', 'last_name', 'dob', 'student_id', 'phone_number', 'school_name', 'transfer_date')):
						res=edit_student_info(field_name,edit_info,student_id)
		else:
			return ("Error request. Improper format.")

	except Exception as e:
		logging.info("Edit student => Improper input format: " + str(e))
		print ("Edit student => Improper input format: " + str(e))
		return ("Improper format", e)

	return res

@app.get("/remove/{student_id}")
def remove_student(student_id):
	""" remove student by student_id """
	""" classes are also removed """

	if (not student_id):
		return ("Error")

	student_id = int(student_id)

	if (student_exist_by_id(student_id) == False): return (f"Student {student_id} does not exist.")
	else:
		student_info = dict(student_info_db.find_one({'student_id':student_id},{'_id':0}))

		# remove student info
		if (student_info_db.delete_one({'student_id':student_id}).deleted_count > 0):
			msg = [f"Student {student_info['first_name'], student_info['last_name']}, id# {student_id} is removed."]
		else:
			return ("Error. Unable to remove student.") 

		# remove student classes
		if (student_grade_db.delete_many({'student_id':student_id}).deleted_count > 0):
			msg.append('Classes are also removed.')
		else:
			msg.append('No class exists for removal.')

		return msg

@app.get("/view")
async def view_all_students(display, page):
	""" View all students """

	display, page = int(display), int(page)

	students = []

	#for row in student_info_db.find({},{'first_name':1,'last_name':1,'dob':1,'student_id':1, '_id':0}).limit(100):		
	for row in student_info_db.find({},{'first_name':1,'last_name':1,'dob':1,'student_id':1, '_id':0}).sort([['student_id',pymongo.DESCENDING]]).skip(page*display).limit(display):		
		students.append(row)

	return students

@app.get("/view_id/{student_id}")
async def view_student_by_id(student_id):
	""" view student record by student_id """

	student_id = int(student_id)

	if student_exist_by_id(student_id):
		student_info = dict(student_info_db.find_one({'student_id':student_id},{'_id':0}))
		student_class = student_grade_db.find({'student_id':student_id},{'_id':0})

		classes = []
		for x in student_class:
			classes.append(dict(x))

		student_info['classes'] = classes
		return (student_info)
	else:
		return ("This student does not exist. Select a different student id number.")


@app.get("/importcsv_one")
def import_student_csv():
	""" read cvs & load into db """

	transcript_url = "student.csv"
	record = pd.read_csv(transcript_url, header=None)

	student = Student()
	student.load_transcript_csv(record)
	return student.import_transcript_db()

@app.get("/importcsv_multiple")
def import_student_csv():
	""" read cvs & load into db """

	import_status = []

	transcript_url = "multiple_students.csv"
	multiple_records = pd.read_csv(transcript_url)

	for i in range(len(multiple_records)):

		# create new dictionary 
		record = {}

		record['first_name'] = str(multiple_records.iloc[i]['first_name'])
		record['last_name'] = str(multiple_records.iloc[i]['last_name'])
		record['dob'] = str(multiple_records.iloc[i]['dob'])
		record['student_id'] = "0"
		record['phone_number'] = str(multiple_records.iloc[i]['phone_number'])
		record['school_name'] = str(multiple_records.iloc[i]['school_name'])
		record['transfer_date'] = str(multiple_records.iloc[i]['transfer_date'])

		try:
			record['classes'] = json.loads(str(multiple_records.iloc[i]['classes']))
		except Exception as e:
			print ("Classes import error.", e)
			record['classes'] = ""
	
		student = Student()
		student.load_transcript_direct(record)
		import_status.append(student.import_transcript_db())

	return import_status


@app.post("/importjson")
def import_student_json(data):
	""" read json & load into db via POST method """

	json_data = json.loads(data)

	student = Student()
	student.load_transcript_direct(json_data)
	return student.import_transcript_db()
