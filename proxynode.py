import socket
from time import sleep
from sys import argv
import rsa
import logging
import json

global privateKey

#Proxy node id for differentiating proxy nodes from one another.
id = "proxy1"

#This is the ip address of the broker that is sending messages to the proxy node, this is set statically.
broker_host_ip = "127.0.0.1"

#Port that listens for messages from brokers (broker --> proxy node). This has to be different since the broker
#will be sending to different proxy nodes at one time, but the ip address can be the same. 
broker_port = None


proxy_node_ip = None
proxy_node_port = None

#Verbose output for more details printed to log.
verbose = False

#Define end of message character and buffer size to hold messages (used for 
#checking if message went through correctly).
EOT_CHAR = b"\4"
BUFFER_SIZE = 1024

#ADDED Code
#This function generates a new entry for the JSON file that we eventually have to send to publishers.
#We need to append to the file for each proxy node that exists with all the relevant information.
#We also need to take into account the fact that multiple proxy nodes may append to this file at 
#the same time, which could be an issue.
def generate_JSON_File():
  (publicKey, privateKey) = rsa.newkeys(1024)
  with open(proxy.json, "r") as file:
    data = json.load(file)
  dictionary = {
    "IP": proxy_node_ip,
    "port": proxy_node_port,
    "ID": id,
    "public-key": publicKey,
    "is-leader": False,
    "is-live": True
}
  data.append(dictionary)

  with open(proxy.json, "w") as file:
    json.dump(data, file)

#ADDED Code
#Added code that can decrypt messages using an associated private or public key (in this case public key).
def decrypt(ciphertext, key):
    try:
        return rsa.decrypt(ciphertext, key).decode('ascii')
    except:
        return False

#ADDED Code
# Added code that can verify message signatures according with SHA-1 signature
def verify(message, signature, key):
    try:
        return rsa.verify(message.encode('ascii'), signature, key,) == 'SHA-1'
    except:
        return False

#Need to add a function to receive messages from broker (and then save to send to leader proxy node).

#Need to add a function to send messages to subscribers.

#Need to add function to handle leader election for proxy nodes.

#Need to write code to handle command line arguments (similar to other two files) to specific proxy node ip address 
#and port number.