import socket
from time import sleep
from sys import argv
import rsa
import logging
import json

global proxy_publicKey
global proxy_privateKey

#Proxy node id for differentiating proxy nodes from one another. We change change this in CLI.
id = "proxy1"

#This is the ip address of the broker that is sending messages to the proxy node, this is set statically.
broker_ip = "127.0.0.1"

#Port that listens for messages from brokers (broker --> proxy node). Broker should be sending to lead proxy
#node and then lead proxy node sends to other proxy nodes.
broker_receiving_port = None

#Port that sends messages to brokers (proxy node --> broker). We use this to send JSON information to the 
#broker.
broker_sending_port = None

#Proxy node needs to know to which subscriber (ip + port) to send its messages to.
proxy_node_sending_ip = None
proxy_node_sending_port = None

#Proxy node needs to know to which (ip + port) to receive messages from the lead proxy node.
proxy_node_receiving_ip = None
proxy_node_sending_port = None

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
def generate_JSON_Dictionary():
  
  #Generate one pair of private and public keys (let's make this the proxy's node's keys)
  (proxy_publicKey, proxy_privateKey) = rsa.newkeys(1024)

  #Generate another pair of private and public keys (let's make this the publisher's keys)
  (pub_publicKey, pub_privateKey) = rsa.newkeys(1024)

  #Maybe we should create a different JSON file that hold publisher ID and public key of the publisher, 
  #that would make it easier for encryption and signature verification since we don't know what proxy 
  #node is going to receive what message from what publisher.

  #NOTE: The below code will now be in the broker.
  with open(proxy.json, "r") as file:
    data = json.load(file)
  #NOTE: This code will now be in the broker.

  dictionary = {
    "IP": proxy_node_ip,
    "port": proxy_node_port,
    "ID": id,
    "public-key": proxy_publicKey,
    "is-leader": False,
    "is-live": True
}

  data = json.dumps(dictionary)

  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    # Setup socket and connect
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    #Connect to the broker's IP address and port to send JSON to the broker.
    s.connect((broker_ip, broker_sending_port))

    # Send message to the broker.
    message = bytes(data,'UTF-8')
    s.sendall(message + EOT_CHAR)

    # Wait for OK response
    return s.recv(BUFFER_SIZE)

  #NOTE: The below code will now be in the broker.
  data.append(dictionary)
  #NOTE: This code will now be in the broker.

  #NOTE: The below code will now be in the broker.
  with open(proxy.json, "w") as file:
    json.dump(data, file)
  #NOTE: This code will now be in the broker.
  


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

#Need to add a function to decrypt + verify messages + send messages to subscribers.

#Need to add function to handle leader election for proxy nodes.

#Need to write code to handle command line arguments (similar to other two files) to specific proxy node ip address 
#and port number.