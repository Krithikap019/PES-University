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

################################################

class User(db.Model):
	id = db.Column(db.Integer, primary_key=True)
	username = db.Column(db.String, unique=True, nullable=False)
	password = db.Column(db.String, unique=False, nullable=False)

	def __init__(self,username,password):
		self.username=username
		self.password=password

class UserSchema(ma.Schema):
	class Meta:
		fields=('id','username','password')

user_schema = UserSchema()
users_schema = UserSchema(many=True)

db.create_all()

#ADD USER
@app.route("/api/v1/users",methods=["PUT","POST"])
def adduser():
	global count_all
	if (request.method != 'POST' and request.method!='PUT'):
		return jsonify({}), 405
	message=request.get_json()
	count_all=count_all+1
	psd=re.compile("(^[abcdefABCDEF0123456789]{40}$)")
	userToAdd=request.get_json()["username"]
	pwd=request.get_json()["password"]

	if(request.method!="PUT"):
		return jsonify({}), 405

	if(not(psd.match(pwd))):
		return jsonify({}), 400 #if password is not valid

	d = dict()
	d['part']=0
	d['flag'] = 1 
	d['username'] = userToAdd
	d['password'] = pwd
	w=requests.post("http://18.214.10.98:80/api/v1/db/read", json=d)
	e =ast.literal_eval(w.text)
	if(e):
		return jsonify({}), 400
	else:
		r=requests.post("http://18.214.10.98:80/api/v1/db/write", json=d)
		return jsonify({}), 201


#API 2 
#DELETE USER
@app.route("/api/v1/users/<username>",methods=["DELETE"])
def delete_user(username):
	global count_all
	if (request.method != 'DELETE'):
		return jsonify({}), 405

	count_all=count_all+1
	d = dict()
	d['part']=0
	d['flag'] = 1
	d['username'] = username
	w=requests.post("http://18.214.10.98:80/api/v1/db/read", json=d)
	e =ast.literal_eval(w.text)
	if(e):
		d['part'] = 0
		d['flag'] = 2
		r=requests.post("http://18.214.10.98:80/api/v1/db/write", json=d)
		return jsonify({}), 200
	else:
		return jsonify({}),400

#SHOW ALL USERS 
@app.route("/api/v1/users",methods=["GET"])
def show():
	global count_all
	count_all=count_all+1
	d=dict()
	d['part'] = 0
	d['flag'] = 222
	r=requests.post("http://18.214.10.98:80/api/v1/db/read", json=d)
	l=ast.literal_eval(r.text)
	return (jsonify(l),200)

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
		



