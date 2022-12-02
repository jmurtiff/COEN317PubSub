import socket
import threading
from time import sleep
from sys import exit, argv
import json
from os.path import exists

#IP address of broker.
host = "127.0.0.1"

#Port that listens for messages from publishers (publisher --> broker)
pub_port = None

#Port that listens for messages from proxy nodes (proxy node --> broker)
proxy_port = None

#Port that listens for messages from subscribers (subscriber --> broker)
#We need this two-way communication if we want the subscribers to easily subscribe or unsubscribe from 
#a given topic.
sub_port = None

#Offset for port sends messages to subscribers --> we need to change this to proxy node.
port_offset = 1

#Verbose output for more details printed to log.
verbose = False

#Define end of message character and buffer size to hold messages (used for 
#checking if message went through correctly).
EOT_CHAR = b"\4"
BUFFER_SIZE = 1024

#Array of subscribers that are connected to the broker, used when a new subscriber
#subscribes or unsubscribes.
subscriptions = []

#Counter for broker to update Lamport timestamp
timestamp = 0

#Log function, prints out broker + specific message
def log(message):
  print("[BROKER] " + message);

#Take in a message from a publisher, and if it matches a specific topic, then send the 
#message to the appropriate subscriber.
#NOTE: This code will not work if the message is encrypted, because we can't take apart the message 
#to know the topic and other elements, so in this case we may have to find a way to allow the topic
#of a message to be transmitted separately so we can send to the correct proxy node.
def handle_pub_message(data):
  data = data.decode().split()
  data = [data[0], data[1], data[2], ' '.join(data[3:])]
  pub_id = data[0]
  topic = data[2]
  message = data[3]
  sub_count = 0
  for sub in subscriptions:
    if sub['topic'] == topic:
      sub_count += 1
      if verbose: log(f"Sending message \"{message}\" to {sub['id']} @ {sub['ip']}:{sub['port']}")
      send_message(message, sub['ip'], sub['port'])
  log(f"{pub_id} published to {topic} ({sub_count} subs): {message}")

#Function to set up socket between broker and subscriber, and then send
#message passed as argument. The socket is TCP, not entirely sure if 
#we want to use TCP or not for this communication. 
#UDP is socket.SOCK_DGRAM instead of socket.SOCK_STREAM
def send_message(message, ip, port):
  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    # Setup socket and connect
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #This is binding the port from subscriber to broker.
    s.bind((host, sub_port + port_offset))

    #Connect to the subscriber's port and IP address --> we need to change this to the proxy node's information
    #once we get the implementation set up.
    connected = False
    while not connected:
      try:
        s.connect((ip, port + port_offset))
        connected = True
      except:
        log("Error on connection. Retrying in 30 seconds...")
        sleep(30)

    # Send message
    message = bytes(message, 'UTF-8')
    s.sendall(message + EOT_CHAR)

    # Wait for OK response
    # return s.recv(BUFFER_SIZE).decode()

#Thread function for one broker to handle multiple publisher messages at the same
#time. Sets up a socket with a single port that all publishers are sending messages 
#through, goes through each socket connection, takes all data, and then sends it 
#to the handle_pub_message() function.
def pubthread():
  log(f"Proxy thread is up at port {proxy_port}")

  while True:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
      # Setup socket and listen for connections
      s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      s.bind((host, pub_port))
      s.listen()

      log("Broker listening on port " + str(pub_port))

      # Accept connections
      conn, addr = s.accept()
      print(addr)
      data = b""
      with conn:
        if verbose: log(f"Publisher connected from {addr[0]}:{addr[1]}")
        # Loop through connections until we get the EOT_CHAR (end-of-transmission)
        while True:
          data += conn.recv(BUFFER_SIZE)
          print("This is the data: " + data.decode())
          if data[-1] == EOT_CHAR[0]:
            data = data[:-1]
            break
        # Send OK response
        # conn.sendall(b"OK")

      ######## LOGIC TO DETECT getProxyNodes header ########

      ######################################################
      handle_pub_message(data)

#Add to the subscriptions array with each subscriber based on their id, topic, ip, and port
#Maybe we can have this list as part of each proxy node to keep track of relevant information for
#their subscribers. 
def subscribe(id, topic, ip, port):
  sub_obj = { "id": id, "topic": topic, "ip": ip, "port": port }
  if sub_obj not in subscriptions:
    subscriptions.append(sub_obj)

#Remove subscriber based on ID or topic name.
def unsubscribe(id, topic):
  global subscriptions
  subscriptions = [s for s in subscriptions if s["id"] != id or s["topic"] != topic]

#This function is used for the subscriber to tell the broker that it wants to subscribe or unsubscribe 
#from a given topic.
def handle_sub_message(data, addr):
  data = data.decode().split()
  sub_id = data[0]
  action = data[1]
  topic = data[2]
  logging_output = "subscribed to" if action == "sub" else "unsubscribed from"
  log(f"{sub_id} {logging_output} {topic}")
  if action == "sub":
    subscribe(sub_id, topic, addr[0], addr[1])
  else:
    unsubscribe(sub_id, topic)
  if verbose: log("Current subs: " + str(subscriptions))


#This function is for communication between broker and subscriber
def subthread():
  log(f"Subscriber thread is up at port {sub_port}")

  while True:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
      # Setup socket and listen for connections
      s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      s.bind((host, sub_port))
      s.listen()

      # Accept connections
      conn, addr = s.accept()
      data = b""
      with conn:
        if verbose: log(f"Subscriber connected from {addr[0]}:{addr[1]}")
        # Loop through connections until we get the EOT_CHAR (end-of-transmission)
        while True:
          data += conn.recv(BUFFER_SIZE)
          if data[-1] == EOT_CHAR[0]:
            data = data[:-1]
            break
        # Send OK response
        conn.sendall(b"OK")
      handle_sub_message(data, addr)

#Handles incoming proxy node messages that are sending JSON information
def proxythread():
  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    # Setup socket and listen for connections
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((host, proxy_port))
    s.listen()

    # Accept connections
    conn, addr = s.accept()
    data = b""
    with conn:
      if verbose: log(f"Proxy node connected from {addr[0]}:{addr[1]}")
      # Loop through connections until we get the EOT_CHAR (end-of-transmission)
      while True:
        data += conn.recv(BUFFER_SIZE)
        if data[-1] == EOT_CHAR[0]:
          data = data[:-1]
          break
      
      # write/append rows of JSON to the broker's replica of proxy.json
      if exists("proxy.json"):
        with open("proxy.json", "a") as file:
          file.write("\n")
          json.dump(json.loads(data.decode("UTF_8")), file)
      else:
        with open("proxy.json", "w") as file:
          json.dump(json.loads(data.decode("UTF-8")), file)
     
#This function is used if we run python broker.py -s sub_port -p pub_port [-o port_offset -v]
#and we enter in a different subscriber port value for the broker via the command line.
def handle_option_sub_port(arguments, i):
  global sub_port
  try:
    sub_port = int(arguments[i+1])
  except: 
    print("Invalid port number")
    return -1

#This function is used if we run python broker.py -s sub_port -p pub_port [-o port_offset -v]
#and we enter in a different publisher port value fore receiving messages for the broker via the command line.
def handle_option_pub_port(arguments, i):
  global pub_port
  try:
    pub_port = int(arguments[i+1])
  except: 
    print("Invalid port number")
    return -1


def handle_option_port_offset(arguments, i):
  global port_offset
  try:
    port_offset = int(arguments[i+1])
  except: 
    print("Invalid port number")
    return -1

def handle_option_verbose(arguments, i):
  global verbose
  verbose = True
  return 1

def handle_option_proxy_port(arguments, i):
  global proxy_port
  try:
    proxy_port = int(arguments[i+1])
  except:
    print("Invalid proxy port number")
    return -1
  return 1

def handle_command_line_args():
  options = {
    "-s": handle_option_sub_port,
    "-p": handle_option_pub_port,
    "-pr": handle_option_proxy_port,
    "-o": handle_option_port_offset,
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

  if not sub_port or not pub_port:
    print("Arguments missing")
    return -1

  return 0

ret_val = handle_command_line_args()
if ret_val != -1:
  log("Broker process started")
  try:
    threading.Thread(target=pubthread).start()
    threading.Thread(target=subthread).start()
    threading.Thread(target=proxythread).start()
  except KeyboardInterrupt:
    exit(0)
else:
  print("Use: python broker.py -s sub_port -p pub_port -pr proxy_port [-o port_offset -v]")