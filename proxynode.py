import socket
from time import sleep
from sys import argv
import rsa
import logging
import json
import threading
import os


# keys for proxy node
proxy_publicKey = None
proxy_privateKey = None

#Proxy node id for differentiating proxy nodes from one another. We change change this in CLI (I still need to write this).
id = None

#This is the ip address of the broker that is messages to the proxy node, this is set statically.
broker_ip = "127.0.0.1"

#Port that sends messages to brokers (proxy node --> broker). We use this to send JSON information to the broker.
# NOTE: This port has to match the "proxy_port" variable in broker.py
broker_port = None

# this is proxy node's IP and port which is used for receiving information from EITHER the broker or proxy node leader
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

#This function generates a new JSON entry that will be sent to the broker to be appended into one JSON file.
#We need to send through the socket all relevant information that a publisher should need
def generate_JSON_Dictionary():
  global proxy_publicKey
  global proxy_privateKey
  #Generate one pair of private and public keys (let's make this the proxy's node's keys)
  (proxy_publicKey, proxy_privateKey) = rsa.newkeys(1024)

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
    s.connect((broker_ip, broker_port))

    # Send message to the broker.
    message = bytes(data,'UTF-8')
    s.sendall(message + EOT_CHAR)

    # Wait for OK response
    return s.recv(BUFFER_SIZE)
  

#If the current proxy node is NOT the intended recipient to perform decryption and verification, then this proxy node
#must be the leader
#Thus, the leader will funnel this message to the other proxy nodes to distribute the work 
def send_message_to_proxy(message, recipient_proxy_ip, recipient_proxy_port):
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

#Added code that can decrypt messages using an associated private or public key (in this case public key).
def decrypt(ciphertext, key):
    try:
        return rsa.decrypt(ciphertext, key).decode('ascii')
    except:
        return False

#Once decryption and verification have been successfully performed, send message to the subscriber 
def send_message_to_subscriber(message, sub_ip, sub_port):
  # connect to subscriber and send message
  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.connect((sub_ip, sub_port))
    s.sendall(message.encode("UTF-8"))
    return s.recv(BUFFER_SIZE)

#This is a helper function intended for cleaner refactoring
def send_message_to_subscribers(message, subscribers):
  for sub in subscribers:
          sub_ip = subscribers[sub]['ip']
          sub_port = subscribers[sub]['port']
          response = send_message_to_subscriber(message, sub_ip, sub_port)

          # resend the message if necessary 
          while response != "OK":
            response = send_message_to_subscriber(message, sub_ip, sub_port)

# Added code that can verify message signatures according with SHA-1 signature
def verify(message, signature, key):
    try:
        return rsa.verify(message.encode('ascii'), signature, key,) == 'SHA-1'
    except:
        return False

# Helper function called by receiverthread() that lets the proxy node store the publisher's public key for verification of signatures
# Format of publisher.json:
# {
#   <publisher ID>: <public_key of publisher>,
#   <publisher ID>: <public_key of publisher>,
#   ...
# }
def store_publisher_public_keys(data):
  # if file already exists, update with new key
  if os.path.exists("publisher.json"):
    with open("publisher.json", "r+") as file:
      data = json.loads(file.read())
      data[data["ID"]] = data["public-key"]
      file.seek(0)
      json.dump(data, file)
      file.truncate()
  else:
    # otherwise instantiate the file and create a new dictionary 
    new_data = { data["ID"]: data["public-key"] }
    with open("publisher.json", "w") as file:
      json.dump(new_data, file)
  

def store_child_proxy_nodes(proxy_list):
  return 
  
def send_elected_messages(proxy_list):
  return

def send_messages():
  return

#This function allows the current proxy node to listen to incoming messages from EITHER the broker or proxy node leader
#2 scenarios that may occur:
# 1) If the current proxy node is the leader (broker is the sender):
#    a) They decrypt the message and send to subscribers if the message is intended for them
#    b) Otherwise they take the 'Proxy-IP' and 'Proxy-Port' provided and send it to child proxy node 
# 2) The current proxy node is accepting incoming message from the leader 
#    a) Accept the message from leader, decrypt/verify message, then send to subscribers
def receiverthread():
  while True:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
      # listen for any incoming communications from the broker
      s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      s.bind((proxy_node_receiving_ip, proxy_node_receiving_port))
      s.listen()

      # Accept connection from EITHER the broker or proxy node leader
      conn, addr = s.accept()
      data = b""
      with conn:
        if verbose: log(f"Broker or proxy node leader connected from {addr[0]}:{addr[1]}")
        # Loop through connections until we get the EOT_CHAR (end-of-transmission)
        while True:
          data += conn.recv(BUFFER_SIZE)
          if data[-1] == EOT_CHAR[0]:
            data = data[:-1]
            break
            
        decoded_data = json.loads(data.decode("UTF-8"))

        # check if leader election has occurred, send elected messages to other proxy nodes, and store information about all proxy nodes 
        if decoded_data["election-message"]:
          store_child_proxy_nodes()
          send_elected_messages()
          
        # If publisher has sent its public key for signing --> store the public key and send to other proxy nodes 
        if "public-key" in decoded_data: 
          store_publisher_public_keys(decoded_data)

        # once all of the data has been received, check the receiving_IP + receiving_PORT in the messages
        if decoded_data['Proxy-IP'] == proxy_node_receiving_ip and decoded_data['Proxy-Port'] == proxy_node_receiving_port:
          decrypted_message = decrypt(decoded_data["Message"], proxy_privateKey)
          verified = verify(decoded_data["Message"], decoded_data["Signature"], proxy_privateKey)
          
          # once decryption and verification is done
          if verified == "SHA-1" and decrypted_message:
            send_message_to_subscribers(decrypted_message, decoded_data["Subscribers"])
        else:
          # this proxy node must be the leader and is NOT the intended recipient, so send the message to other proxy node
          response = send_message_to_proxy(data, decoded_data['Proxy-IP'], decoded_data['Proxy-Port'])

          # resend the message if necessary 
          while response != "OK":
            response = send_message_to_proxy(data, decoded_data['Proxy-IP'], decoded_data['Proxy-Port'])

def handle_proxy_id(arguments, i):
  global id
  try:
    id = int(arguments[i + 1])
  except:
    print("Invalid proxy ID")
    return -1
  return 1 
 
def handle_option_proxy_port(arguments, i):
  global proxy_node_receiving_port
  try:
    proxy_node_receiving_port = int(arguments[i+1])
  except:
    print("Invalid proxy port number")
    return -1
  return 1

def handle_option_proxy_ip(arguments, i):
  global proxy_node_receiving_ip
  try:
    proxy_node_receiving_ip = arguments[i+1]
  except:
    print("Invalid proxy IP")
    return -1
  return 1

def handle_broker_ip(arguments, i):
  global broker_ip
  try:
    broker_ip = arguments[i+1]
  except:
    print("Invalid broker IP")
    return - 1
  return 1 

def handle_broker_port(arguments, i):
  global broker_port
  try:
    broker_port = int(arguments[i+1])
  except:
    print("Invalid broker port")
    return - 1
  return 1 

#This thread handles messages for leader election 
def leaderelectionthread():
  return 

def handle_command_line_args():
  options = {
    "i": handle_proxy_id,
    "-ip": handle_option_proxy_ip,
    "-p": handle_option_proxy_port,
    "-b": handle_broker_ip,
    "-br": handle_broker_port,
  }

  arguments = argv[1:]
  i = 0
  while i < len(arguments):
    if arguments[i] in options.keys():
      try:
        ret_val = options[arguments[i]](arguments, i)
      except:
        print("Invalid input")
        return -1
      if ret_val == -1:
        return -1
      elif ret_val == 1:
        i -= 1
    i += 2

  if not id or not broker_ip or not broker_port:
    print("Arguments missing")
    return -1


# handle command line arguments when creating the proxy node 
ret_val = handle_command_line_args()
if ret_val != -1:
  log("Proxy node process started")
  #Create thread for receiving communications from broker/proxy node leader
  try:
    threading.Thread(target=receiverthread).start()
  except KeyboardInterrupt:
    exit(0)
else:
    print("Use: python3 proxynode.py -i proxy_ID -b broker_IP -bs broker_port -ip proxy_IP -p proxy_port")

#Need to add function to handle leader election for proxy nodes.
#Bully algorithm based on IDs --> information that it needs for other proxy nodes is in JSON file 

#generate_JSON_Dictionary is done first
generate_JSON_Dictionary()
#Sleep for some times until broker has complete JSON file for all proxy nodes + publisher proxy nodes 
sleep(10)
