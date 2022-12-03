import socket
from time import sleep
from sys import argv
import logging
import json
import random
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
from Crypto.Hash import SHA1
from Crypto.Signature import pkcs1_15
import base64
import time

#This is good, doesn't have to change.
#Publisher id for differentiating publishers between one another. We can change these values
#at run time to something different if we want to.
id = "p1"

#This is good, doesn't have to change.
#This is the ip and port # of the broker that the publisher is sending to, we can change 
#this value to whatever we want it to be.
broker_ip = None
broker_port = None

#ADDED CODE
#This is good, doesn't have to change.
#The proxy node IP address and port, we get this from the JSON file that the proxy nodes
#have created and was sent to us by the broker.
proxy_ip = None
proxy_port = None
proxy_node_count = 0

#This is good, doesn't have to change.
#Verbose output for more details printed to log. 
#NOTE: How do we do logging functions in Python? It keeps giving me errors whenever I try to run the log function.
verbose = False

#Define end of message character and buffer size to hold messages (used for 
#checking if message went through correctly).
EOT_CHAR = b"\4"
BUFFER_SIZE = 1024

#Log function, prints out broker + specific message
def log(message):
  print("[PUBLISHER] " + message);

#ADDED CODE
#This is good, doesn't have to change.
#This function takes in a message and either the public or private key and encrypts the message
#using the private or public key.
def encrypt(message, key):
  start_time = time.time()
  cipher_rsa = PKCS1_OAEP.new(key)
  ciphertext = cipher_rsa.encrypt(message.encode())
  end_time = time.time()
  print("Encryption time: ", round(start_time-end_time, 4), sep="")
  return ciphertext

#ADDED CODE
#This is good, doesn't have to change.
#This function takes in a message and a key and signs the message using SHA-1.
def sign(message, key):
  start_time = time.time()
  newMessage = message.encode('UTF-8')
  h = SHA1.new(newMessage)
  signature = pkcs1_15.new(key).sign(h)
  end_time = time.time()
  print("Signing time: ", round(start_time-end_time, 4), sep="")
  return signature

#QUESTION: Why do we need to include clientIP and clientPort for message?
def get_proxy_nodes():
  global proxy_node_count

  # construct the request to be sent to the broker; NOTE: can set client port to be something else
  request = json.dumps({
    "getProxyNodes": True,
    "clientIP": broker_ip,
    "clientPort": broker_port
  })

  # connect to the broker, send the request, and then receive the data from the broker's proxy.json replica
  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((broker_ip, broker_port))
    request = request.encode("UTF-8")
    s.sendall(request + EOT_CHAR)
    
    data = b""
    # because we've connected to the broker, we can read in the broker's reply (JSON file contents)
    data = s.recv(BUFFER_SIZE)
    data = data.decode("UTF-8")
    proxy_node_count = data.count("{") # the number of '{' indicates how many rows of JSON there are

    # after receiving all of the JSON data, dump that JSON data into own proxy.json replica
    with open("proxy.json", "w") as file:
      file.write(data)
  

#ADDED Code
#This function generates a pair of private and public keys and sends the publicKey as well as the
#publisher's ID to the broker to generate a JSON file that can be used when we send messages to
#the broker and to know which publicKey we should use when the proxy node has to verify the message 
#sent to it.
def generate_JSON_ID_PublicKey():
  global publicKey, privateKey

  keypair = RSA.generate(2048)

  #This is the public key in PublicKey form.
  publicKey = keypair.publickey()

  #This is the private key in PrivateKey form.
  privateKey = keypair

  #Export public key
  exported_pub_publicKey = publicKey.export_key('PEM')

  #Turn exported private key into string.
  final_pub_publicKey = exported_pub_publicKey.decode('utf-8')

  dictionary = {
    "ID": id,
    "public-key": final_pub_publicKey
  }

  data = json.dumps(dictionary)

  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    # Setup socket and connect
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    #Connect to the broker's IP address and port to send JSON information to the broker.
    s.connect((broker_ip, broker_port))

    # Send message to the broker.
    message = bytes(data,'UTF-8')
    s.sendall(message + EOT_CHAR)
    # Wait for OK response
    return s.recv(BUFFER_SIZE).decode()



#Function to set up socket between publisher and broker, and then send
#message passed as argument. The socket is TCP, not entirely sure if 
#we want to use TCP or not for this communication. 
#UDP is socket.SOCK_DGRAM instead of socket.SOCK_STREAM
def send_message(message,topic):
  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    # Setup socket and connect

    #Next line is used to avoid "Address already in use error" presumably between
    #when the code is executed multiple times.
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    #Connect to the broker's IP address and port to send messages to the broker.
    s.connect((broker_ip, broker_port))

    # Send message to the broker.
    # message = message.encode("UTF-8")

    #Is this return value a list or a dictionary?
    random_proxy_node = random.randint(1, proxy_node_count) - 1

    with open("proxy.json", "r") as file:
      for index, line in enumerate(file):
        if index == random_proxy_node:
          line = json.loads(line)
          # get the public key from there
          proxy_ip = line['IP']
          proxy_port = line['port']
          proxy_public_key = line['public-key']

    #We need to sign using the private key of the publisher.
    signature = base64.b64encode(sign(message,privateKey))
    signature = signature.decode("UTF-8")

    proxy_public_key = RSA.import_key(proxy_public_key)
    #We need to encrypt using the public key of the proxy node, which we have to read from the JSON file itself.
    encrypted_message = base64.b64encode(encrypt(message,proxy_public_key))
    encrypted_message = encrypted_message.decode("UTF-8")

    #Create a partially encrypted message that contains relevant information as well as the message the 
    #publisher creates.
    final_message = {
      "Payload": encrypted_message,
      "Publisher-ID": id,
      "Proxy-IP": proxy_ip,
      "Proxy-Port": proxy_port,
      "Topic": topic,
      "Signature": signature
    }

    final_message = json.dumps(final_message)
  
    s.sendall(final_message.encode() + EOT_CHAR)

    # Wait for OK response
    return s.recv(BUFFER_SIZE)


#Publish function logs and then calls send_message to send message to a broker.
#Acknowledges a message has been received by broker if verbose output is enabled, 
#Message includes publisher id, the topic, as well as the message itself.
def publish(topic, message):
  log(f"Publishing to {topic}: {message}")
  response = send_message(id + " pub " + topic + " " + message, topic)
  if verbose: log(f"Received {response.decode()} from broker")

#This function checks to see whether the user has entered an incorrect command line interface command.
def check_command(command):
  return not command[0].isdigit() or int(command[0]) < 0 or len(command) < 4 or command[1] != "pub"

#Calls publish function based on series of commands, command[0] is wait time, command[1] is referring to publisher,
#command[2] is topic for publishing, and command[3] is message that will be published
def handle_command(command):
  command = [command[0], command[1], command[2], ' '.join(command[3:])]
  topic = command[2]
  message = ' '.join(command[3:])
  if(int(command[0]) > 0):
    if verbose: log(f"Waiting {command[0]} second(s)...")
    sleep(int(command[0]))
  publish(topic, message)

#Instead of handling one command at a time, take in commands from a file 
#and execute them in order. Useful for testing purposes.
def handle_command_file():
  file = open(command_file, "r").readlines()
  for command in file:
    command = command.replace("\n", "")
    if verbose: log(f"Running command from file: \"{command}\"")
    command = command.split(" ")
    handle_command(command)

#While the publisher process is running, we can enter in commands via the command line
#interface in order to make the publisher send messages with a specific topic, message, 
#and wait time before sending messages. This runs indefinitely so we can keep entering commands
#as we wish.
def handle_cli_commands():
  try:
    while True:
      log("Enter command:")
      command = input().split()
      while check_command(command):
        log("Invalid command")
        log("Use: <wait time> <pub> <topic> <message>")
        command = input().split(" ")
      handle_command(command)
  except KeyboardInterrupt:
    return

#This is good, doesn't have to change.
#This function is used if we run "python publisher.py -i ID -r pub_port -h broker_IP -p port [-f command_file -v]""
#and we enter in a different ID value for the publisher via the command line.
def handle_option_id(arguments, i):
  global id
  id = arguments[i+1]

#This is good, doesn't have to change.
#This function is used if we run "python publisher.py -i ID -r pub_port -h broker_IP -p port [-f command_file -v]""
#and we enter in a different publisher client port value for the publisher via the command line.
def handle_option_client_port(arguments, i):
  global client_port
  try:
    client_port = int(arguments[i+1])
  except: 
    print("Invalid port number")
    return -1

#This is good, doesn't have to change.
#This function is used if we run "python publisher.py -i ID -r pub_port -h broker_IP -p port [-f command_file -v]""
#and we enter in a different broker IP address for the publisher via the command line.
def handle_option_broker_ip(arguments, i):
  global broker_ip
  broker_ip = arguments[i+1]

#This is good, doesn't have to change.
#This function is used if we run "python publisher.py -i ID -r pub_port -h broker_IP -p port [-f command_file -v]""
#and we enter in a different broker port # for the publisher via the command line.
def handle_option_broker_port(arguments, i):
  global broker_port
  try:
    broker_port = int(arguments[i+1])
  except: 
    print("Invalid port number")
    return -1

#This is good, doesn't have to change.
#This function is used if we run "python publisher.py -i ID -r pub_port -h broker_IP -p port [-f command_file -v]""
#and we enter in a unique command file that is executed with several commands.
def handle_option_command_file(arguments, i):
  global command_file
  command_file = arguments[i+1]

#This is good, doesn't have to change.
#This function is used if we run "python publisher.py -i ID -r pub_port -h broker_IP -p port [-f command_file -v]""
#and we enter in the verbose option to include more runtime information.
def handle_option_verbose(arguments, i):
  global verbose
  verbose = True
  return 1


def handle_command_line_args():

  #Options here each call functions depending on the arguments that are entered 
  #by the user.
  options = {
    "-i": handle_option_id,
    "-r": handle_option_client_port,
    "-h": handle_option_broker_ip,
    "-p": handle_option_broker_port,
    "-f": handle_option_command_file,
    "-v": handle_option_verbose,
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

  if not id or not client_port or not broker_ip or not broker_port:
    print("Arguments missing")
    return -1

  return 0

#If we enter a correct CLI instance of a publisher process, then we execute either a command file
#(to tell the publisher what to do) or we enter in commands one by one for the publisher to execute.
ret_val = handle_command_line_args()
if ret_val != -1:
  log("Publisher process started")
  get_proxy_nodes()
  generate_JSON_ID_PublicKey()
  log("SENT PUBLISHER'S PUBLIC KEY")
  #handle_command_file()
  handle_cli_commands()
else:
  print("Use: python publisher.py -i ID -r pub_port -h broker_IP -p port [-f command_file -v]")