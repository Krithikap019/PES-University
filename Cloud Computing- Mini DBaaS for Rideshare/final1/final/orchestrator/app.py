import logging
from kazoo.client import KazooClient
from kazoo.client import KazooState
import pika
import sys
import uuid
from flask import jsonify
from flask import request
import math
import threading
import time
import docker
from time import time as timer
import os
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
from flask_restful import Resource, Api
import json
import re
from datetime import datetime
import requests
import ast
from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

time.sleep(10)
client = docker.DockerClient(base_url='unix://var/run/docker.sock')
app = Flask(__name__)

######### GLOBAL VARIABLES #########
global bool_user
global count_all
global how_die
global count
global slave_name
global worker_pids
global pidlist
global masterpid
global children
global slavelist
slavelist=[]
corr_id=""
response=None
count_all=0
childcount=0
count=0
timeout=120
slave_name=0
how_die=0

######### LEADER ELECTION #########
def leader_election():
    global children
    global masterpid
    global slavelist
    slavelist=[]
    zk.set("/producer/goingon",b"1")
    listofkids=[]
    #FIND LIST OF CHILDREN
    for x in children:
        if (x!='goingon'):
            listofkids.append(int(x))
    #FIND MASTERPID
    masterpid=min(listofkids)
    pathname="/producer/"+str(masterpid)
    zk.set(pathname,b"1")
    #SET ALL SLAVES TO 0
    for x in listofkids:
       if(str(x)!='goingon' and str(x)!=str(masterpid)):
           slavelist.append(x)
           p="/producer/"+str(x)
           zk.set(p,b"0")

######### RPC RESPONSE QUEUE FUNCTION #########
def onreadresponse(ch, method, props, body):
    global response
    global bool_user
    if(corr_id == props.correlation_id):
        response=body.decode("utf-8")
        if(response=="0" or response=="1"):
         bool_user=response
        elif(response=="3"):
         r=True
         bool_user={'val':r}
        elif(response=="4"):
         r=False
         bool_user={'val':r}
        else:
          r=json.loads(response)
          if(type(r)==list):
            bempz=json.loads(response)
            clim=[]
            for x in bempz:
              clim=x
            bool_user=jsonify(clim)
            response=json.dumps(r)
          else:
            response=json.loads(body)
            l=[]
            for i in range(len(response)):
              index=str(i)
              l.append(response[index])
            bool_user=jsonify(l)
            response=json.dumps(l)
        ch.queue_declare(queue='sendtomaster',durable=True)
        ch.basic_publish(exchange='',routing_key='sendtomaster',body=response)

def newCont():
    global count
    global slavelist
    global slave_name
    global children
    global masterpid
    global how_die
    ct=count/20
    num=math.ceil(ct)
    count=0
    n=0
    templist=[]
    for vee in children:
       if(vee!='goingon' and vee!=str(masterpid)):
          templist.append(vee)
    print(templist,file=sys.stderr)
    slave_len=len(templist)
    ######### NO NEW SLAVES NEEDED #########
    if(num==slave_len):
        print('')
    ######### NEW SLAVES NEEDED #########
    elif(slave_len<num):
        while(num-slave_len>0):
            slave_len+=1
            newname='slave'+str(slave_name)
            slave_name+=1
            slave_container=client.containers.run('final_master',name=newname,environment=["PYTHONBUFFERED=1"],network="final_default",volumes={'/home/ubuntu/final1/final/master/': {'bind': '/app', 'mode': 'rw'},'/usr/bin/docker':{'bind':'/usr/bin/docker'},'/var/run/docker.sock':{'bind':'/var/run/docker.sock'}},detach=True)
    ######### NEED TO DECREASE SLAVES #########
    else:
        if(slave_len!=1):
            slavelist_num=[]
            for v in templist:
               slavelist_num.append(v)
            while(slave_len-num>0):
                if(slave_len==1):
                    break
                pid_rem=max(slavelist_num)
                slavelist_num.remove(pid_rem)
                command="docker ps -q | xargs docker inspect --format \'{{.State.Pid}}, {{.Id}}\' | grep "+str(pid_rem)+" > pidtemp.txt"
                os.system(command)
                with open('pidtemp.txt','r') as ll:
                    linez=ll.readlines()
                linez=[x.strip() for x in linez]
                for tempvar in linez:
                   strrr=tempvar
                result=strrr.split(",")[1]
                contid_rem=result.strip()
                k=client.containers.get(contid_rem)
                k.kill()
                how_die=1
                path_del="/producer/"+str(pid_rem)
                zk.delete(path_del)
                slave_len-=1
            
######### TIMER #########
def mytime():
    deadline = timer() + timeout # reset
    while True:
        if (deadline < timer()): # timeout
            deadline = timer() + timeout # reset
            newCont()

######### ZOOKEEPER INITIALIZATION #########
logging.basicConfig()
zk = KazooClient(hosts='zoo1:2181')
zk.start()
zk.delete("/producer", recursive=True)

time.sleep(20)
zk.ensure_path("/producer")
zk.create("/producer/goingon",b"0")
children = zk.get_children("/producer")
print("There are %s children with names %s" % (len(children), children),file=sys.stderr)
leader_election()
zk.set("/producer/goingon",b"0")
print(masterpid,file=sys.stderr)
pathname="/producer/"+str(masterpid)

######### CHILDWATCH #########
@zk.ChildrenWatch("/producer")
def watch_children(child):
    global children
    global masterpid
    global how_die
    global slave_name
    print("Children are now: %s" % child,file=sys.stderr)
    #CHILDREN HAVE INCREASED
    if(len(children)<len(child)):
         children=child
         childcount=len(child)
         for x in children:
            if(str(x)!='goingon' and str(x)!=str(masterpid)):
                p="/producer/"+str(x)
                zk.set(p,b"0")
    #CHILDREN HAVE DECREASED
    elif(len(children)>len(child)):
        children=child
        if(how_die==1):
            how_die=0
        else:
            newname="slave"+str(slave_name)
            slave_name+=1
            slave_container=client.containers.run('final_master',name=newname,environment=["PYTHONBUFFERED=1"],network="final_default",volumes={'/home/ubuntu/final1/final/master/': {'bind': '/app', 'mode': 'rw'},'/usr/bin/docker':{'bind':'/usr/bin/docker'},'/var/run/docker.sock':{'bind':'/var/run/docker.sock'}},detach=True)
            

######### CRASH SLAVE #########
@app.route('/api/v1/crash/slave',methods=['POST'])
def crashslave():
    global children
    global maasterpid
    global how_die
    templist2=[]
    for vee2 in children:
       if(vee2!='goingon' and vee2!=str(masterpid)):
          templist2.append(vee2)
    pid_rem2=max(templist2)            
    command2="docker ps -q | xargs docker inspect --format \'{{.State.Pid}}, {{.Id}}\' | grep "+str(pid_rem2)+" > container_id.txt"
    os.system(command2)
    with open('container_id.txt','r') as ll2:
        linez2=ll2.readlines()
    linez2=[x.strip() for x in linez2]
    for tempvar2 in linez2:
        strrr2=tempvar2
    result2=strrr2.split(",")[1]
    contid_rem2=result2.strip()            
    k2=client.containers.get(contid_rem2)
    k2.kill()
    path_del2="/producer/"+str(pid_rem2)
    zk.delete(path_del2)
    return jsonify({"message":"slave crashed"}),200

########## CRASH MASTER #########
@app.route('/api/v1/crash/master',methods=['POST'])
def crashmaster():
    global maasterpid
    global how_die
    how_die=1
    pid_rem2=masterpid  
    command2="docker ps -q | xargs docker inspect --format \'{{.State.Pid}}, {{.Id}}\' | grep "+str(pid_rem2)+" > master_container_id.txt"
    os.system(command2)
    with open('master_container_id.txt','r') as ll2:
        linez2=ll2.readlines()
    linez2=[x.strip() for x in linez2]
    for tempvar2 in linez2:
        strrr2=tempvar2
    result2=strrr2.split(",")[1]
    contid_rem2=result2.strip()            
    k2=client.containers.get(contid_rem2)
    k2.kill()
    path_del2="/producer/"+str(pid_rem2)
    zk.delete(path_del2)
    return jsonify({"message":"master crashed"}),200
   
########## LIST OF WORKERS #########
@app.route('/api/v1/worker/list',methods=['POST'])
def workerlist():
    global children
    worker_list=[]
    for x in children:
        if(x!='goingon'):
            worker_list.append(int(x))
    worker_list.sort()
    return jsonify(worker_list),200

########## READ API #########
@app.route('/api/v1/db/read',methods=['POST'])
def readfunc():
    global corr_id
    global response
    global count_all
    global bool_user
    global count
    count_all+=1
    count+=1
    message=request.get_json()
    message=json.dumps(message)

    #TIMER
    if(count_all==1):
       t = threading.Thread(target =mytime)
       t.start()
    response=None
    
    #CONNECT TO PIKA
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    #READ AND RESPONSE QUEUE
    channel.queue_declare(queue='readQ',durable=True)
    channel.queue_declare(queue='responseQ',durable=True)
    channel.basic_consume(queue='responseQ', on_message_callback=onreadresponse,auto_ack=True)
    channel.basic_publish(exchange='', routing_key='readQ', body=message, properties=pika.BasicProperties(reply_to='responseQ', correlation_id=corr_id, delivery_mode=2))
    while response is None:
     connection.process_data_events()
    connection.close()
    return bool_user

########## WRITE API #########
@app.route('/api/v1/db/write',methods=['POST'])
def addz():
    connection2 = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel2 = connection2.channel()

    #QUEUES
    channel2.exchange_declare(exchange='writeE')
    channel2.queue_declare(queue='writeQ', durable=True)
    message=request.get_json()
    message=json.dumps(message)
    channel2.basic_publish(exchange='writeE', routing_key='writeQ', body=message)
    channel2.exchange_declare(exchange='syncE', exchange_type='fanout')
    channel2.basic_publish(exchange='syncE', routing_key='', body=message)
    connection2.close()
    return jsonify({})

if __name__ == '__main__':
    app.run(debug=True,use_reloader=False,host='0.0.0.0')








