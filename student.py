# app.py
import pandas as pd 
import pymongo, random, json
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.responses import JSONResponse

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["students_transfer"]
student_info_db = mydb["students_info"]
student_grade_db = mydb["students_grade"]

#student_info_db.delete_many({})
#student_grade_db.delete_many({})

app = FastAPI()

class Student():

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
		""" student info from csv other than classes """
		if (row[0] == 'first_name'): self.first_name = row[1].strip().title() + '_' + str(random.randint(100,250))
		elif (row[0] == 'last_name'): self.last_name = row[1].strip().title() + '_' + str(random.randint(100,250))
		elif (row[0] == 'dob'): self.dob = row[1].strip().title()
		elif (row[0] == 'phone_number'): self.phone_number = row[1].strip().title()
		elif (row[0] == 'school_name'): self.school_name = row[1].strip().title()
		elif (row[0] == 'transfer_date'): self.transfer_date = row[1].strip().title()
		else:
			print ("")

	def prprepare_student_classes(self, row):
		""" student classes from csv """
		temp = dict()
		temp['class'] = row[0]
		temp['grade'] = row[1]
		temp['term'] = row[2]
		temp['date'] = row[3]
		
		self.classes.append(temp)

	def load_transcript_csv(self, records):
		""" load student transcript: student info & student classes """
		for i in range(len(records)):
			current_record = records.iloc[i]

			# pass in student info
			if (i < 7): self.prepare_student_info(current_record)

			# pass in student classes
			elif (i >= 9): self.prprepare_student_classes(current_record)

	def load_transcript_direct(self, records):
		if ('first_name' in records): self.first_name = first_name
		if ('last_name'): self.last_name = last_name
		if ('dob'): self.dob = dob
		if ('phone_number'): self.phone_number = phone_number
		if ('school_name'): self.school_name = school_name
		if ('transfer_date'): self.transfer_date = transfer_date
		if ('classes'): self.classes = classes

	def next_student_id(self):
		if (student_info_db.count_documents({}) == 0):
			return 1000
		else:
			last_id = student_info_db.find({},{'student_id':1}).sort([['student_id',pymongo.DESCENDING]]).limit(1)
			for row in last_id:
				return row['student_id']+1

	def check_student_exist(self):
		my_query = {"first_name":self.first_name,"last_name":self.last_name,"dob":self.dob}
		return student_info_db.count_documents(my_query)

	def check_student_class_exist(self,student_id,class_name,class_term):
		my_query = {"student_id":student_id,"class":class_name,"term":class_term}
		self.notes.append({5:f"[info] {class_name} exists. Duplicate."})
		return student_grade_db.count_documents(my_query)

	def import_transcript_db(self):
		""" import data to db """

		# convert to dictionary & check for error
		record = self.check_transcript()
		if (self.notes and min(self.notes) == 0): return (self.notes)
		else:
			# check if student exists

			if (self.check_student_exist() > 0):
				print ("Student already exists. No import.")
				return (f"Student {self.first_name, self.last_name} already exists. No import.")
			else:
				
				# student_info
				print (f"Importing student {self.first_name, self.last_name} info to student db")
				next_student_id = self.next_student_id()

				record['student_id'] = next_student_id
				record_student_info = {key:val for key,val in record.items() if key != 'classes'}

				#print (record_student_info)

				record_student_info['student_id'] = next_student_id	# assign student_id
				student_info_db.insert_one(record_student_info)

				# class_info
				if (not record['classes']):
					print ("No classes to import.")
				else:
					print ("Importing student classes to classes db")
					for one_class in record['classes']:
						this_class = {ind:val for ind, val in one_class.items()}
						this_class['student_id'] = next_student_id

						#print (this_class)
						if (self.check_student_class_exist(next_student_id, this_class['class'], this_class['term']) > 0):
							print ("[Error] This class exists. Not importing.")
						else:
							student_grade_db.insert_one(this_class)

				return record

	def check_transcript(self):
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
			if (not val and key not in ('classes','transfer_date','phone_number','school_name')):
				#print (f"Error. Empty value in '{key}'")
				self.notes.append({0:f"[red flag] Error. '{key}' is required."})

			elif (not val):
				self.notes.append({5:f"[info] {key} is missing data."})

		return (record)



def edit_student_info(self, field_name, field_value_change):
	pass

def edit_classes_info(self, field_name, field_value_change):
	pass

def student_exists(student_id):
	return True if student_info_db.count_documents({"student_id":student_id}) > 0 else False

#print (student_grade_db.delete_many({'student_id':1027}).deleted_count)

@app.get("/",response_class=HTMLResponse)
async def default():
	return "import: <a href='./importcsv'>csv</a>, <a href='./importapi'>api</a>  | view: <a href='./view'>all</a> <a href='./view_id/1000'>by id</a> | <a href='./remove/1000'>remove</a>"

@app.get("/edit/")
def edit_student(rservice,rtype,field_name,new_info):
	if (rservice == 'info'):
		dbase = student_info_db
	elif (rservice == 'classes'):
		dbase = student_grade_db
	else:
		return ("Error request.")



@app.get("/remove/{student_id}")
def remove_student(student_id):
	if (not student_id):
		return ("Error")

	student_id = int(student_id)
	if (student_exists(student_id) == False):
		return (f"Student {student_id} does not exist.")
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
async def view_all_students():
	students = []
	#for row in student_info_db.find({},{'first_name':1,'last_name':1,'dob':1,'student_id':1, '_id':0}).limit(100):		
	for row in student_info_db.find({},{'first_name':1,'last_name':1,'dob':1,'student_id':1, '_id':0}).sort([['student_id',pymongo.DESCENDING]]):		

		#student_classes = []
		#for class_row in student_grade_db.find({'student_id':row['student_id']},{'_id':0, 'student_id':0}):
		#	student_classes.append(class_row)

		#row['classes'] = student_classes
		students.append(row)

	return JSONResponse(content=students)

@app.get("/view_id/{student_id}")
async def view_student_by_id(student_id):
	student_id = int(student_id)

	if student_exists(student_id):
		student_info = dict(student_info_db.find_one({'student_id':student_id},{'_id':0}))
		student_class = student_grade_db.find({'student_id':student_id},{'_id':0})

		classes = []
		for x in student_class:
			classes.append(dict(x))

		student_info['classes'] = classes
		return (student_info)
	else:
		return ("This student does not exist. Select a different student id number.")


@app.get("/importcsv")
def import_student_csv():
	transcript_url = "student.csv"

	record = pd.read_csv(transcript_url, header=None)
	student = Student()
	student.load_transcript_csv(record)
	return student.import_transcript_db()


@app.get("/importapi")
def import_student_api():

	return ("Need to pass in API url with equivalent info.")

	api_url = "http://api"
	record = pd.read_csv(transcript_url, header=None)

	student = Student()
	student.load_transcript_csv(record)
	return student.import_transcript_db()
