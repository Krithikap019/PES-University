
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
from flask_restful import Resource, Api
import json
import os
import re
from constant import Area
from datetime import datetime
import requests
import ast
from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.orm import relationship

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
global create_all
count_all=0

################################################
#Flask App
app = Flask(__name__)
basedir=os.path.abspath(os.path.dirname(__file__))
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///"+os.path.join(basedir,'db.sqlite')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
ma = Marshmallow(app)
api=Api(app)


class Rides(db.Model):
	rideId = db.Column(db.Integer, primary_key=True)
	created_by = db.Column(db.String, unique=False, nullable=False)
	timestamp = db.Column(db.String)
	source = db.Column(db.Integer, nullable=False)
	destination = db.Column(db.Integer, nullable=False)

	def __init__(self,created_by,timestamp,source,destination):
			self.created_by=created_by
			self.timestamp=timestamp
			#self.users=[users]

			self.source=source
			self.destination=destination

class RideSchema(ma.Schema):
	class Meta:
		fields=('rideId','created_by','timestamp','source','destination')

ride_schema = RideSchema()
rides_schema = RideSchema(many=True)

################################################

class Other_Users(db.Model):
	i = db.Column(db.Integer, primary_key=True)
	ID = db.Column(db.Integer, unique=False,nullable=False)
	user_names= db.Column(db.String, nullable=False)

	def __init__(self,ID,user_names):
		self.ID=ID
		self.user_names=user_names

class Other_UsersSchema(ma.Schema):
	class Meta:
		fields=('ID','user_names')

other_user_schema = Other_UsersSchema()
other_users_schema = Other_UsersSchema(many=True)

###############################################
db.create_all()

#API 3
#ADD RIDE
@app.route("/api/v1/rides",methods=["POST"])
def addride():
	global count_all
	if (request.method != 'POST'):
		return jsonify({}), 405
	count_all=count_all+1
	timeptrn=re.compile("((0[1-9]|[12][0-9]|3[01])-(0[13578]|1[02])-(18|19|20)[0-9]{2})|(0[1-9]|[12][0-9]|30)-(0[469]|11)-(18|19|20)[0-9]{2}|(0[1-9]|1[0-9]|2[0-8])-(02)-(18|19|20)[0-9]{2}|29-(02)-(((18|19|20)(04|08|[2468][048]|[13579][26]))|2000):[0-5][0-9]-[0-5][0-9]-(2[0-3]|[01][0-9])")

	created_by=request.get_json()["created_by"]
	timestamp=request.get_json()["timestamp"]
	source=request.get_json()["source"]
	destination=request.get_json()["destination"]

	try:
		datetime.strptime(timestamp, '%d-%m-%Y:%S-%M-%H')
	except:
		return jsonify({}), 400
	
	s=int(source) in Area._value2member_map_
	desti=int(destination) in Area._value2member_map_

	if(not(timeptrn.match(timestamp)) or not(s) or not(desti)):
		return 'timstamp2', 400 #if time is not valid
	
	d = dict()
	d['created_by'] = created_by
	d['timestamp'] = timestamp
	d['source'] = source
	d['destination'] = destination
	d['flag'] = 2
	d['part'] = 1
	w=requests.post("http://18.214.10.98:80/api/v1/db/read", json=d)
	e =ast.literal_eval(w.text)
	if(e):
		d['flag'] = 3
		r=requests.post("http://18.214.10.98:80/api/v1/db/write", json=d)
		return jsonify({}), 201
	else :
		return jsonify({}), 400

#API 7
#DELETE RIDE
@app.route("/api/v1/rides/<ride_id>",methods=["DELETE"])
def deleterride(ride_id):
	global count_all
	if (request.method != 'DELETE'):
		return jsonify({}), 405
	count_all=count_all+1
	d=dict()
	d['flag']=3
	d['rideId']=ride_id
	d['part']=1
	w=requests.post("http://18.214.10.98:80/api/v1/db/read", json=d)
	e=ast.literal_eval(w.text)
	if(e):
		d['flag']=4
		d['part']=1
		r=requests.post("http://18.214.10.98:80/api/v1/db/write", json=d)
		return jsonify({}), 200
	else :
		return jsonify({}), 400

#SHOW ALL RIDES {FOR REFERENCE}
@app.route("/api/v1/db/showall")
def showall():
	all_rides=Rides.query.all()
	res=rides_schema.dump(all_rides)
	return jsonify(res)

########################################################

#API 4 : Remaining : printing format
#View ride details using source and destination
@app.route("/api/v1/rides",methods=["GET"])
def viewridesource():
	global count_all
	if (request.method != 'GET'):
		return jsonify({}), 405
	count_all=count_all+1
	source=request.args['source']
	dest =request.args['destination']

	d=dict()
	d['source']=source
	d['destination']=dest
	d['flag']= 7
	d['part']=1
	s=int(source) in Area._value2member_map_
	desti=int(dest) in Area._value2member_map_
	if(s and desti ):
		rr1=requests.post("http://18.214.10.98:80/api/v1/db/read", json=d)
		r =ast.literal_eval(rr1.text)
		if(r):
			return json.dumps(r), 200
		else:
			return jsonify({}), 204
	else:
		return 'invalid source/dest', 400


#########################################################
#API 5
#DISPLAY RIDE DETAILS
@app.route("/api/v1/rides/<ride_id>", methods=["GET"])
def viewridedetails(ride_id):
	global count_all
	if (request.method != 'GET'):
		return jsonify({}), 405

	count_all=count_all+1
	l=[]
	d = dict()
	d['part']=1
	d['flag'] = 3 #check if rideId exists in rides db
	d['rideId'] = ride_id
	exus1=requests.post("http://18.214.10.98:80/api/v1/db/read", json=d)
	exus =ast.literal_eval(exus1.text)
	strss=exus
	di=dict()
	di['part']=1
	di['flag'] = 4  #to get ride details
	di['rideId'] = ride_id
	ride1=requests.post("http://18.214.10.98:80/api/v1/db/read", json=di)
	ride =ast.literal_eval(ride1.text)
	
	dit = dict()
	dit['part']=1
	dit['flag'] =  5 #check if rideId 
	dit['rideId'] = ride_id
	ursr1=requests.post("http://18.214.10.98:80/api/v1/db/read", json=dit)
	ursr =ast.literal_eval(ursr1.text)
	
	dito = dict()
	dito['part']=1
	dito['flag'] = 6 #copy her 8 th one
	dito['rideId'] = ride_id
	r1=requests.post("http://18.214.10.98:80/api/v1/db/read", json=dito)
	r =r1.json()
	r2=r['val']
	if(r2):
                ride_to=ride_schema.jsonify(ride)
                dz=dict()
                dz['part']=1
                dz['flag']=777
                dz['rideId']=ride_id
                extract=requests.post("http://18.214.10.98:80/api/v1/db/read", json=dz)
                extract =ast.literal_eval(extract.text)
                ridiss=str(extract[0])
                cbss=str(extract[1])
                tsss=str(extract[2])
                sss=str(extract[3])
                dss=str(extract[4])
                dr={"rideId":''.join(ridiss),"created_by":''.join(cbss),"timestamp":''.join(tsss),"source":''.join(sss),"destination":''.join(dss)}
                ride_to=json.dumps(dr)
                usrss_to=other_user_schema.jsonify(ursr)
                dz['part']=1
                dz['flag']=7777
                usrss_to=requests.post("http://18.214.10.98:80/api/v1/db/read", json=dz)
                usrss_to =ast.literal_eval(usrss_to.text)
                usrss_to=json.dumps(usrss_to)
                dict1ss=eval(ride_to)
                dict2ss=eval(usrss_to)
                dict3ss={**dict1ss,**dict2ss}
                reslt=json.dumps(dict3ss)
                fnss={"rideId":dict3ss.get("rideId"),"created_by":dict3ss.get("created_by"),"users":dict3ss.get("usernames"),"timestamp":dict3ss.get("timestamp"),"source":dict3ss.get("source"),"destination":dict3ss.get("destination")}
                reslt=json.dumps(fnss)
                return reslt,200
	else:
                if(request.method!="GET"):
                        return jsonify({}), 405
                else:
                        return jsonify({}), 204


#SHOW ALL USERS {FOR REFERENCE}
@app.route("/api/v1/users",methods=["GET"])
def show():
	all_users=User.query.all()
	re=users_schema.dump(all_users)
	if(not re):
		return (jsonify({}),204)
	if(request.method!="GET"):
		return (jsonify({}), 405)
	l=[]
	for i in re:
		for k in i.keys():
			if(k=="username"):
				l.append(i[k])

	return (jsonify(l),200)


#########################################################


@app.route("/api/v1/rides/redirect", methods=["POST"])
def redirect():
        flag = request.get_json()["flag"]
        username = request.get_json()["username"]
        d = dict()
        d['flag'] = 2
        d['username'] = username
        r=requests.post("http://127.0.0.1:5000/api/v1/db/write", json=d)
        return (jsonify({}), 201)


#########################################################
#API 6
#Add other users
@app.route("/api/v1/rides/<rideId>", methods=["POST"])
def add_otheruser(rideId):
	global count_all
	if (request.method != 'POST'):
		return jsonify({}), 405

	count_all=count_all+1
	user_name=request.get_json()["username"]
	d = dict()
	d['part']=1
	d['rideId']= rideId
	d['username']=user_name

	d['flag'] = 1 #check if user exists
	u=requests.post("http://18.214.10.98:80/api/v1/db/read", json=d)
	e1=ast.literal_eval(u.text)
	d['part']=1
	d['flag'] = 3 #check if ride exists
	u=requests.post("http://18.214.10.98:80/api/v1/db/read", json=d)
	e2=ast.literal_eval(u.text)
	d['part']=1
	d['flag'] = 8 #check if the user to be added is the used who created the ride
	u=requests.post("http://18.214.10.98:80/api/v1/db/read", json=d)
	e3=ast.literal_eval(u.text)
	d['part']=1
	d['flag'] = 9 #check if the user to be added is talready in users
	u=requests.post("http://18.214.10.98:80/api/v1/db/read", json=d)
	e4=ast.literal_eval(u.text)

	#and e5
	if(e1 and e2 and e3 and e4):
		d['flag']=5
		d['part']=1
		r=requests.post("http://18.214.10.98:80/api/v1/db/write", json=d)
		return jsonify({}), 200
	else :
		return jsonify({}), 400

#########################################################

#CLEAR DB
@app.route("/api/v1/db/clear",methods=["POST"])
def clear_data():
	if (request.method != 'POST'):
		return jsonify({}), 405
	global count_all
	count_all=count_all+1
	d=dict()
	d['flag']=23
	d['part']=0
	r=requests.post("http://18.214.10.98:80/api/v1/db/write",json=d)
	return jsonify(),200

#########################################################

#######CRASH SLAVE########
@app.route('/api/v1/crash/slave')
def crashslave():
	r=requests.post("http://18.214.10.98:80/api/v1/crash/slave")
	return jsonify({}),200

######CRASH MASTER#######
@app.route('/api/v1/crash/master')
def crashmaster():
	r=requests.post("http://18.214.10.98:80/api/v1/crash/master")
	return jsonify({}),200

######WORKER LIST#######
@app.route('/api/v1/worker/list',methods=['GET'])
def workerlist():
	r=requests.post("http://18.214.10.98:80/api/v1/worker/list")
	worker_list =ast.literal_eval(r.text)
	return jsonify(worker_list),200



#Show the count of number of of rides
@app.route("/api/v1/rides/count",methods=["GET"])
def countrides():
	d=dict()
	d['part']=1
	d['flag']=1234
	r=requests.post("http://18.214.10.98:80/api/v1/db/read", json=d)
	llv =ast.literal_eval(r.text)
	final=[llv]
	return jsonify(final), 200

@app.route("/api/v1/_count",methods=["GET"])
def returncount():
	l=[]
	l.append(count_all)
	return jsonify(l),200

@app.route("/api/v1/_count",methods=["DELETE"])
def resetcount():
	global count_all
	count_all=0
	return jsonify({}),200


if __name__ == '__main__':
	app.debug=True
	app.run(host="0.0.0.0")
		



