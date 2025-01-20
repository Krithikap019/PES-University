import pika
from flask import Flask, render_template,session, jsonify, request, abort, redirect
from flask_sqlalchemy import SQLAlchemy
from flask_session import Session
from flask_marshmallow import Marshmallow
from flask_restful import Resource, Api
import re
from constant import Area
from datetime import datetime
import requests
import ast
import random
import csv
import requests
import json
import atexit
import time
import docker
import logging
import os
from kazoo.client import KazooClient
from kazoo.client import KazooState
app = Flask(__name__)
######### GLOBAL VARIABLES DECLARATION #########
global isMaster
global cont_id
global count
count=0
isMaster=2

######### CLEAR DAATABASE FUNCTION, IF THE DB ALREADY EXISTS #########
def clear_data():
  meta = db.metadata
  for table in reversed(meta.sorted_tables):
    db.session.execute(table.delete())
  db.session.commit()
 
logging.basicConfig()
time.sleep(20)
client = docker.DockerClient(base_url='unix://var/run/docker.sock')
c=docker.APIClient()


######### FIND ITS PID #########
def find_pid():
        global cont_id
        cmd='hostname>t.txt'
        os.system(cmd)
        with open('t.txt','r') as l:
            sline=l.readlines()
        sline=[x.strip() for x in sline]
        cont_id=sline[0]
        return c.inspect_container(cont_id)['State']['Pid']

pid=find_pid()
print("pid: ",pid)
dbname="db"+str(pid)+".sqlite"

######### PATH OF DATABASE #########
basedir=os.path.abspath(os.path.dirname(__file__))
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///"+os.path.join(basedir,dbname)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
ma = Marshmallow(app)
api=Api(app)
c = app.test_client()
time.sleep(5)

######### ZOOKEEPER CONNECTIONS #########
zk = KazooClient(hosts='zoo1:2181')
zk.start()
zk.ensure_path("/producer")



######### DATABASE SCHEMA #########
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


######### IF DB ALREADY EXISTS, CLEAR IT #########
if(os.path.isfile(dbname)):
 clear_data()

 ######### CREATE DB #########
db.create_all()


######### PRINT CONTENTS OF USERS DB #########
def print_db_users():
 all_users=User.query.all()
 re=users_schema.dump(all_users)
  #print(re)
  #re=[]
 if(not re):
  print("empty db")
 else:
  l=[]
  for i in re:
   for k in i.keys():
    if(k=="username"):
     l.append(i[k])
  print(l)


######### RETURNS LIST OF ALL USERS #########
def list_db_users():
 all_users=User.query.all()
 re=users_schema.dump(all_users)
 l=[]
 if(not re):
  return l
 else:
  for i in re:
   for k in i.keys():
    if(k=="username"):
     l.append(i[k])
  return l


######### ZOOKEEPER - DECLARE NODE #########
contpath="/producer/"+str(pid)

if zk.exists(contpath):
    print("Node already exists")
else:
    print("creating node")
    zk.create(contpath, b"-1",ephemeral=True)

data, stat = zk.get(contpath)
print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))


######### ALL READ REQUESTS COME HERE #########
def readcallback(ch, method, props, body):
    print("I am servicing this request!")
    message=json.loads(body)
    part=message["part"]
    flag = message["flag"]

    #CHECK IF USERNAME IS PRESENT
    if(flag == 1 and part==0):  
        username = message["username"]
        u=bool(User.query.filter_by(username = username).first())
        if(u):
         resp="1"
        else:
         resp="0"

    #CHECK IF CREATEBY IS PRESENT IN USERS DB
    elif (flag == 2 and part ==1): 
        username = message["created_by"]
        l=list_db_users()
        if(username in l):
         resp="1"
        else:
         resp="0"

    #GET ALL USERS API
    elif(flag == 222 and part==0):
         l=list_db_users()
         l=[l]
         usrss_to=json.dumps(l)
         resp=usrss_to

    #CHECK IF RIDE IS PRESENT
    elif (flag == 3 and part==1):
        rideId = message["rideId"]
        u=bool(Rides.query.filter_by(rideId = rideId).first())
        if(u):
            resp="1"
        else:
            resp="0"

    elif(flag==8 and part==1):
      rideId=message['rideId']
      username=message['username']
      r=bool(Rides.query.filter_by(rideId = rideId, created_by=username).first())
      if(r):
        resp="0"
      else:
        resp="1"

    elif(flag==1 and part==1):
      username = message["username"]
      l=list_db_users()
      if(username in l):
        resp="1"
      else:
        resp="0"

    elif(flag==9 and part==1):
      rideId=message['rideId']
      username=message['username']
      r=bool(Other_Users.query.filter_by(ID = rideId, user_names=username).first())
      if(r):
       resp="0"
      else:
       resp="1"

    elif(flag==7777 and part==1):
         ride_id=message['rideId']
         usr = Other_Users.query.all()
         l=[]
         for urs in usr:
           if(urs.ID == int(ride_id)):
             l.append(urs.user_names)
         d={"usernames":l}
         dd=[d]
         usrss_to=json.dumps(dd)
         resp=usrss_to

    elif (flag == 4 and part==1):
        rideId = message["rideId"]
        ride = db.session.query(Rides.rideId, Rides.created_by, Rides.timestamp,Rides.source, Rides.destination).filter_by(rideId = rideId).all()
        resp=json.dumps(ride)

    elif (flag == 5 and part==1):
     rideId = message["rideId"]
     ursr= db.session.query(Other_Users.user_names).filter_by(ID = rideId).all()
     resp=json.dumps(ursr)

    elif (flag == 6 and part==1):
     rideId = message["rideId"]
     r=bool(Rides.query.filter_by(rideId = rideId).first())
     if(r==True):
       x=3
     else:
       x=4
     resp=json.dumps(x)

    elif(flag== 777 and part==1):
     rideId=message["rideId"]
     fkme=db.session.query(Rides.rideId,Rides.created_by,Rides.timestamp,Rides.source,Rides.destination).filter_by(rideId = rideId).one()
     l=[]
     l.append(fkme)
     resp=json.dumps(l)

     #COUNT RIDES
    elif(flag==1234 and part==1):
      all_rides=Rides.query.all()
      llv=len(all_rides)
      l=[]
      l.append(llv)
      resp=str(l)

    elif (flag == 7 and part ==1):
        sourceok=message['source']
        dest =message['destination']
        now=datetime.utcnow()
        s1=now.strftime("%d-%m-%Y:%S-%M-%H")
        d1=datetime.strptime(s1, "%d-%m-%Y:%S-%M-%H")
        srcc = db.session.query(Rides).filter_by(source = sourceok, destination=dest).with_entities(Rides.rideId,Rides.created_by,Rides.timestamp).all()
        temp = srcc.copy()
        for use in temp:
         s2=use.timestamp
         d2=datetime.strptime(s2,"%d-%m-%Y:%S-%M-%H")
         k=str(d2-d1)
         user_data={}
         if(k[0]=="-"):
          srcc.remove(use)
        r_schema = RideSchema(many=True)
        ress= r_schema.dump(srcc)
        dnew=dict()
        final={}
        index=0
        for i in ress:
         print()
         dnew=dict()
         for k in i.keys():
          dnew["username"]=i["created_by"]
          dnew["timestamp"]=i["timestamp"]
          dnew["rideId"]=i["rideId"]
         final[index]=dnew
         index+=1
        resp=final
        resp=json.dumps(resp)
    ch.basic_publish(exchange='',routing_key='responseQ',body=resp,properties=pika.BasicProperties( correlation_id=props.correlation_id,delivery_mode=2))
    ch.basic_ack(delivery_tag=method.delivery_tag)
    

def writecallback2(ch, method, properties, body):
    print("")

######### ALL WRITE REQUESTS COME HERE #########
def writecallback(ch, method, properties, body):
    message=json.loads(body)
    part=message['part']
    flag = message["flag"]
    #INSERT USER
    if( flag == 1 and part == 0): 
        username = message["username"]
        password = message["password"]
        newUser=User(username,password)
        db.session.add(newUser)
        db.session.commit()

     #CLEAR DB
    elif(flag==23) :
         clear_data()

    #DELETE USER IN USERS
    elif (flag == 2 and part ==0) : 
        username = message["username"]
        User.query.filter_by(username=username).delete()
        d = dict()
        d['part']=1
        d['flag'] =2
        d['username'] = username
        l=requests.post("http://18.214.10.98:80/api/v1/db/write",json=d)
        db.session.commit()
        
    #DELETE USER IN RIDES    
    elif (flag == 2 and part==1) : 
        username = message["username"]
        Other_Users.query.filter_by(user_names=username).delete()
        Rides.query.filter_by(created_by=username).delete()
        db.session.commit()

    #ADD RIDE    
    elif (flag == 3 and part==1):
        hey=message["created_by"]
        timestamp=message["timestamp"]
        source=message["source"]
        destination=message["destination"]
        newRide=Rides(hey,timestamp,source,destination)
        db.session.add(newRide)
        db.session.commit()

    #DELETE RIDE
    elif (flag == 4 and part==1) : 
        rideId = message["rideId"]
        Rides.query.filter_by(rideId=rideId).delete()
        Other_Users.query.filter_by(ID=rideId).delete()
        db.session.commit()

    elif(flag==5 and part==1):
      ID = message["rideId"]
      username = message["username"]
      new=Other_Users(ID,username)
      db.session.add(new)
      db.session.commit()

######### DATA WATCH #########
@zk.DataWatch(contpath)
def watch_node(data, stat):
    global isMaster
    global masterpid
    data=data.decode("utf-8")
    if("0"==data):
         isMaster=0
    elif("1"==data):
         isMaster=1
         masterpid=pid

######### DATA WATCH FOR LEADER ELECTION #########
@zk.DataWatch("/producer/goingon")
def watch_node(data, stat):
    global isMaster
    global count
    global masterpid
    if(count==0):
       count+=1
    elif(count==1):
       count+=1
    elif(count==2):
       data, stat = zk.get(contpath)
       data=data.decode("utf-8")
       if (data=="1"):
           masterpid=pid
           isMaster=1
       else:
           isMaster=0


######### CONNECT TO PIKA #########
sleepTime = 20
print(' [*] Sleeping for ', sleepTime, ' seconds.')
print(' [*] Connecting to server ...')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

######### LOOP THROUGH THIS UNTIL ELECTION #########
while True:
    if(isMaster!=2):
        break

######### IN MASTER #########
if(isMaster==1):
        print("in master==1")
        ######### WRITE QUEUE #########
        channel.exchange_declare(exchange='writeE')
        channel.queue_declare(queue='writeQ', durable=True)
        channel.queue_bind(exchange='writeE', queue='writeQ')
        channel.basic_consume(queue='writeQ', on_message_callback=writecallback, auto_ack=True)
        channel.queue_declare(queue='sendtomaster',durable=True)
        channel.basic_consume(queue='sendtomaster',on_message_callback=writecallback2,auto_ack=True)


######### IN SLAVE #########
elif(isMaster==0):
        print("in master==0")
        if(count==2):
         print('')
        else:
        #FINDING MASTER PID
         children= zk.get_children("/producer")
         for x in children:
          if(x=='goingon'):
           continue
          path_child="/producer/"+str(x)
          data,stat=zk.get(path_child)
          data=data.decode("utf-8")
          if(data=="0"):
           continue
          if(data=="1"):
           masterpid=x
         #COPY MASTER DATABASE
         command="cp db"+str(masterpid)+".sqlite db"+str(pid)+".sqlite"
         os.system(command)
         #LOAD DB AGAIN
         basedir=os.path.abspath(os.path.dirname(__file__))
         app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///"+os.path.join(basedir,dbname)
         app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
         db = SQLAlchemy(app)

        ######### READ QUEUE #########
        channel.queue_declare(queue='readQ', durable=True)
        channel.queue_declare(queue='responseQ', durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='readQ', on_message_callback=readcallback)

         ######### SYNC QUEUE #########
        channel.exchange_declare(exchange='syncE', exchange_type='fanout')
        result = channel.queue_declare(queue='', exclusive=True,durable=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange='syncE', queue=queue_name)
        channel.basic_consume(queue=queue_name, on_message_callback=writecallback,auto_ack=True)

channel.start_consuming()









