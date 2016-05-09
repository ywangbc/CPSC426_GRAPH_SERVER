#!/usr/bin/env python

import socket

# Set up a TCP/IP socket
s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)

# Connect as client to a selected server
# on a specified port
s.connect(("40.114.7.48",6999))

add_node1 = "{\"node_id\": 123}" 
add_node2 = "{\"node_id\": 456}" 
add_node3 = "{\"node_id\": 789}" 

get_node1 = "{\"node_id\": 123}" 

add_edge1 = "{\"node_id\": 123, \"node_id\": 456}" 
add_edge2 = "{\"node_id\": 456, \"node_id\": 789}" 

get_edge1 = "{\"node_id\": 123, \"node_id\": 456}" 

shortest_path1 = "{\"node_id\": 123, \"node_id\": 789}"

remove_edge1 = "{\"node_id\": 456, \"node_id\": 789}" 

shortest_path2 = "{\"node_id\": 123, \"node_id\": 789}"

# Protocol exchange - sends and receives
s.send("POST www.example.com:8000/api/v1/get_node HTTP/1.1\r\n")
s.send("Content-Length: "+str(len(get_node1)+1)+"\r\n")
s.send("Content-Type: application/json"+"\r\n\r\n")
s.send(get_node1+"\r\n")
while True:
    resp = s.recv(1024)
    if "node_id" in resp:
        print resp
        break
    elif "204" in resp:
        print resp
        break
    elif "in_graph" in resp:
        print resp
        break
    else:
        print resp,


# Protocol exchange - sends and receives
s.send("POST www.example.com:8000/api/v1/get_edge HTTP/1.1\r\n")
s.send("Content-Length: "+str(len(get_edge1)+1)+"\r\n")
s.send("Content-Type: application/json"+"\r\n\r\n")
s.send(get_edge1+"\r\n")
while True:
    resp = s.recv(1024)
    if "node_id" in resp:
        print resp
        break
    elif "204" in resp:
        print resp
        break
    elif "in_graph" in resp:
        print resp
        break
    else:
        print resp,

s.close()
print "\ndone"
