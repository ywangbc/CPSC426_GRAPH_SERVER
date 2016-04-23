#!/usr/bin/env python

import socket

# Set up a TCP/IP socket
s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)

# Connect as client to a selected server
# on a specified port
s.connect(("40.114.7.48",6999))

add_node3 = "{\"node_id\": 789}" 
add_edge2 = "{\"node_id\": 456, \"node_id\": 789}" 

remove_node1 = "{\"node_id\": 789}"
remove_edge1 = "{\"node_id\": 456, \"node_id\": 789}"

remove_node1 = "{\"node_id\": 789}" 

print "Sixth round started"
s.send("POST www.example.com:8000/api/v1/add_node HTTP/1.1\r\n")
s.send("Content-Length: "+str(len(add_node3)+1)+"\r\n")
s.send("Content-Type: application/json"+"\r\n\r\n")
s.send(add_node3+"\r\n")
while True:
    resp = s.recv(1024)
    if "node_id" in resp:
        print resp
        break
    elif "204" in resp:
        print resp
        break
    else:
        print resp

# Protocol exchange - sends and receives
print "Seven round started"
s.send("POST www.example.com:8000/api/v1/add_edge HTTP/1.1\r\n")
s.send("Content-Length: "+str(len(add_edge2)+1)+"\r\n")
s.send("Content-Type: application/json"+"\r\n\r\n")
s.send(add_edge2+"\r\n")
while True:
    resp = s.recv(1024)
    if "200" in resp:
        print resp
        break
    elif "204" in resp:
        print resp
        break
    elif "400" in resp:
        print resp
        break
    else:
        print resp,


# Protocol exchange - sends and receives
print "Eighth round started"
s.send("POST www.example.com:8000/api/v1/remove_edge HTTP/1.1\r\n")
s.send("Content-Length: "+str(len(remove_edge1)+1)+"\r\n")
s.send("Content-Type: application/json"+"\r\n\r\n")
s.send(remove_edge1+"\r\n")
while True:
    resp = s.recv(1024)
    if "200" in resp:
        print resp
        break
    elif "204" in resp:
        print resp
        break
    elif "400" in resp:
        print resp
        break
    else:
        print resp,

print "Sixth round started"
s.send("POST www.example.com:8000/api/v1/remove_node HTTP/1.1\r\n")
s.send("Content-Length: "+str(len(remove_node1)+1)+"\r\n")
s.send("Content-Type: application/json"+"\r\n\r\n")
s.send(remove_node1+"\r\n")
while True:
    resp = s.recv(1024)
    if "node_id" in resp:
        print resp
        break
    elif "204" in resp:
        print resp
        break
    else:
        print resp
