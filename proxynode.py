import socket
from time import sleep
from sys import argv
import rsa
import logging
import json
import threading

global proxy_publicKey
global proxy_privateKey

#Proxy node id for differentiating proxy nodes from one another. We change change this in CLI (I still need to write this).
id = "proxy1"

#This is the ip address of the broker that is sending messages to the proxy node, this is set statically.
broker_ip = "127.0.0.1"

#Port that listens for messages from brokers (broker --> proxy node). Broker should be sending to lead proxy
#node and then lead proxy node sends to other proxy nodes.
broker_receiving_port = None

#Port that sends messages to brokers (proxy node --> broker). We use this to send JSON information to the 
#broker.
broker_sending_port = None

#Proxy node needs to know to which subscriber (ip + port) to send the appropriate messages to.
proxy_node_sending_ip = None
proxy_node_sending_port = None

#Proxy node needs to know to which (ip + port) to receive messages from the lead proxy node (also helps with
#handling leader election).
proxy_node_receiving_ip = None
proxy_node_receiving_port = None

#Verbose output for more details printed to log.
verbose = False

#Define end of message character and buffer size to hold messages (used for 
#checking if message went through correctly).
EOT_CHAR = b"\4"
BUFFER_SIZE = 1024

#Log function, prints out broker + specific message
def log(message):
  print("[PROXY NODE] " + message);

#ADDED Code
#This function generates a new JSON entry that will be sent to the broker to be appended into one JSON file.
#We need to send through the socket all relevant information that a publisher should need
def generate_JSON_Dictionary():
  
  #Generate one pair of private and public keys (let's make this the proxy's node's keys)
  (proxy_publicKey, proxy_privateKey) = rsa.newkeys(1024)

  #Generate another pair of private and public keys (let's make this the publisher's keys)
  (pub_publicKey, pub_privateKey) = rsa.newkeys(1024)

  #Maybe we should create a different JSON file that hold publisher ID and public key of the publisher, 
  #that would make it easier for encryption and signature verification since we don't know what proxy 
  #node is going to receive what message from what publisher.

  #NOTE: The below code will now be in the broker.
  with open("proxy.json", "r") as file:
    data = json.load(file)
  #NOTE: This code will now be in the broker.

  dictionary = {
    "IP": proxy_node_receiving_ip,
    "port": proxy_node_receiving_port,
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
  

#If the current proxy node is NOT the intended recipient to perform decryption and verification, then this proxy node
#must be the leader
#Thus, the leader will funnel this message to the other proxy nodes to distribute the work 

def send_message(message, recipient_proxy_ip, recipient_proxy_port):
  """
  message: the original bytestream of the message received from the socket
  recipient_proxy_ip: the intended recipient's IP
  recipient_proxy_port: the intended recipient's port
  """

  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    # connect to the intended proxy node
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.connect((recipient_proxy_ip, recipient_proxy_port))

    # once connected, send the message to recipient proxy node and receive the response
    s.sendall(message)
    return s.recv(BUFFER_SIZE).decode("UTF-8")

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

#Need to add a function to continually receive messages from broker (like a thread) and then send to leader proxy node.
#Need to make sure that leader proxy node has receiving proxy node ip and port (so it knows where to forward messages to).
def brokerthread():
  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    # listen for any incoming communications from the broker or possibly leader proxy node
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((proxy_node_receiving_ip, proxy_node_receiving_port))
    s.listen()

    # Accept connection from the broker
    conn, addr = s.accept()
    data = b""
    with conn:
      if verbose: log(f"Broker connected from {addr[0]}:{addr[1]}")
      # Loop through connections until we get the EOT_CHAR (end-of-transmission)
      while True:
        data += conn.recv(BUFFER_SIZE)
        if data[-1] == EOT_CHAR[0]:
          data = data[:-1]
          break
          
      # once all of the data has been received, check the receiving_IP + receiving_PORT in the messages
      data = json.loads(data.decode("UTF-8"))
      decoded_data = json.loads(data.decode("UTF-8"))

      # if this proxy node is the intended recipient, perform the decryption and verification 
      if decoded_data['Proxy-IP'] == proxy_node_receiving_ip and decoded_data['Proxy-Port'] == proxy_node_receiving_port:
        decrypted = decrypt(decoded_data["Message"], proxy_privateKey)
        verified = verify(decoded_data["Message"], decoded_data["Signature"], proxy_privateKey)
      else:
        # this proxy node must be the leader and is NOT the intended recipient, so send the message to other proxy node
        response = send_message(data, decoded_data['Proxy-IP'], decoded_data['Proxy-Port'])

        # resend the message if necessary 
        while response != "OK":
          response = send_message(data, decoded_data['Proxy-IP'], decoded_data['Proxy-Port'])


#Instantiate any threads here and have them continuously running
try:
  threading.Thread(target=brokerthread).start()
except KeyboardInterrupt:
  exit(0)
#Need to add a function to decrypt + verify messages + send messages to subscribers.

#Need to add function to handle leader election for proxy nodes.
#Bully algorithm based on IDs --> information that it needs for other proxy nodes is in JSON file 


#Need to write code to handle command line arguments (similar to other two files) to specific proxy node ip address 
#and port number.

#Proxy node is being created
#generate_JSON_Dictionary is done first
#Sleep for some times until broker has complete JSON file for all proxy nodes + publisher proxy nodes 

#When the first proxy node, 