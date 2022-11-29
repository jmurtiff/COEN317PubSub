import socket
from time import sleep
from sys import argv
import rsa
import logging

global publicKey
global privateKey

#Publisher id for differentiating publishers between one another. We can change these values
#at run time to something different if we want to.
id = "p1"

#This is the ip and port # of the publisher itself, this is set statically. And is only used 
#if the publisher wants to listen for messages from itself??? Not sure exactly why its here.
client_ip = "127.0.0.1"
client_port = 8001

#This is the ip and port # of the broker that the publisher is sending to, we can change 
#this value to whatever we want it to be.
server_ip = None
server_port = None

#ADDED CODE
#The proxy node that the publisher sends its public key to has to be known ahead of time, because
#how else can this be done? We should probably add the proxy node's ip and port # as CLI arguments
#Unless we somehow can assign proxy nodes based on topics somehow???
proxy_ip = None
proxy_port = None

#Verbose output for more details printed to log.
verbose = False

#Define end of message character and buffer size to hold messages (used for 
#checking if message went through correctly).
EOT_CHAR = b"\4"
BUFFER_SIZE = 1024

#ADDED CODE
#This function is necessary to generate public and private keys for RSA encryption.
def generateKeys():
  (publicKey, privateKey) = rsa.newkeys(1024)
  return privateKey, publicKey

#ADDED CODE
#This function takes in a message and either the public or private key and encrypts the message
#using the private or public key.
def encrypt(message, key):
  return rsa.encrypt(message.encode('ascii'), key)

#ADDED CODE
#This function takes in a message and a key and signs the message using SHA-1.
def sign(message, key):
  return rsa.sign(message.encode('ascii'), key, 'SHA-1')


#Function to set up socket between publisher and broker, and then send
#message passed as argument. The socket is TCP, not entirely sure if 
#we want to use TCP or not for this communication. 
#UDP is socket.SOCK_DGRAM instead of socket.SOCK_STREAM
def send_message(message):
  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    # Setup socket and connect

    #Next line is used to avoid "Address already in use error" presumably between
    #when the code is executed multiple times.
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    #Listen to the publisher itself --> Not entirely sure why this is needed. 
    s.bind((client_ip, client_port))

    #Connect to the broker's IP address and port to send messages to the broker.
    s.connect((server_ip, server_port))

    # Send message to the broker.
    message = bytes(message, 'UTF-8')

    #NOTE:Need to encrypt message using RSA before sending it, but we don't want to encrypt sending the public
    # key to the proxy node first. So we need a variable here that has a socket connection with a proxy node first
    #to send the key to an established proxy node before sending it to the broker that sends it to the same proxy node.
    
    #NOTE: We can create a new function to handle this first before we call send_message(message), but then afterwards we 
    #have to handle telling the broker to what proxy node to send the information to. 

    s.sendall(message + EOT_CHAR)

    # Wait for OK response
    return s.recv(BUFFER_SIZE)



#ADDED CODE
def send_proxynode_message():
  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    # Setup socket and connect

    #Next line is used to avoid "Address already in use error" presumably between
    #when the code is executed multiple times.
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    #Connect to proxy node ip address and port number 
    s.connect((proxy_ip, proxy_port))

    # Send public key to the proxy node for later decryption of messages. 
    message = publicKey
    message = bytes(message, 'UTF-8')
    s.sendall(message + EOT_CHAR)



#Publish function logs and then calls send_message to send message to a broker.
#Acknowledges a message has been received by broker if verbose output is enabled, 
#Message includes publisher id, the topic, as well as the message itself.
#NOTE: This code will be a problem if we encrypt it as the message as the return value will
#not be able to deceiver the different parts of the message. We need to change this function.
def publish(topic, message):
  log(f"Publishing to {topic}: {message}")
  response = send_message(id + " pub " + topic + " " + message)
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
      command = input()
      while check_command(command):
        log("Invalid command")
        log("Use: <wait time> <pub> <topic> <message>")
        command = input().split(" ")
      handle_command(command)
  except:
    return

    return

#This function is used if we run "python publisher.py -i ID -r pub_port -h broker_IP -p port [-f command_file -v]""
#and we enter in a different ID value for the publisher via the command line.
def handle_option_id(arguments, i):
  global id
  id = arguments[i+1]

#This function is used if we run "python publisher.py -i ID -r pub_port -h broker_IP -p port [-f command_file -v]""
#and we enter in a different publisher client port value for the publisher via the command line.
def handle_option_client_port(arguments, i):
  global client_port
  try:
    client_port = int(arguments[i+1])
  except: 
    print("Invalid port number")
    return -1

#This function is used if we run "python publisher.py -i ID -r pub_port -h broker_IP -p port [-f command_file -v]""
#and we enter in a different broker IP address for the publisher via the command line.
def handle_option_server_ip(arguments, i):
  global server_ip
  server_ip = arguments[i+1]

#This function is used if we run "python publisher.py -i ID -r pub_port -h broker_IP -p port [-f command_file -v]""
#and we enter in a different broker port # for the publisher via the command line.
def handle_option_server_port(arguments, i):
  global server_port
  try:
    server_port = int(arguments[i+1])
  except: 
    print("Invalid port number")
    return -1


def handle_option_command_file(arguments, i):
  global command_file
  command_file = arguments[i+1]

#This function is used if we run "python publisher.py -i ID -r pub_port -h broker_IP -p port [-f command_file -v]""
#and we enter in the verbose option to include more runtime information.
def handle_option_verbose(arguments, i):
  global verbose
  verbose = True
  return 1


def handle_command_line_args():

  #Options here each call functions depending on the CLI entered by the user.
  options = {
    "-i": handle_option_id,
    "-r": handle_option_client_port,
    "-h": handle_option_server_ip,
    "-p": handle_option_server_port,
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

  if not id or not client_port or not server_ip or not server_port:
    print("Arguments missing")
    return -1

  return 0

#If we enter a correct CLI instance of a publisher process, then we execute either a command file
#(to tell the publisher what to do) or we enter in commands one by one for the publisher to execute.
ret_val = handle_command_line_args()
if ret_val != -1:
  log("Publisher process started")

  #We need to generate public and private RSA keys, and send to the associated proxy node before we start sending to
  #the broker node.
  #NOTE: This code is not yet complete.
  generateKeys()
  send_proxynode_message()


  handle_command_file()
  handle_cli_commands()
else:
  print("Use: python publisher.py -i ID -r pub_port -h broker_IP -p port [-f command_file -v]")