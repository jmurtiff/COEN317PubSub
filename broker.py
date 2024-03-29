import socket
import threading
from time import sleep
from sys import exit, argv
import json
from os.path import exists
import time

#IP address of broker.
host = "192.168.137.11"

#Port that listens for messages from publishers (publisher --> broker)
pub_port = None

#Port that listens for messages from proxy nodes (proxy node --> broker)
proxy_port = None

#Global variable for proxy_leader ip
proxyleader_ip = None
proxyleader_port = None

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

#Take in a message from a publisher and route to the known proxy leader
#NOTE: This code will not work if the message is encrypted, because we can't take apart the message 
#to know the topic and other elements, so in this case we may have to find a way to allow the topic
#of a message to be transmitted separately so we can send to the correct proxy node.
def handle_pub_message(data):
  global proxyleader_ip
  global proxyleader_port
  
  # if proxyleader hasn't been set, then we have to block any incoming requests until it is done 
  while proxyleader_ip is None or proxyleader_port is None:
    pass

  #Find ip and port of leader proxy node, from proxy.json
  with open("proxy.json", "r") as infile:
    for line in infile:
        dict = json.loads(line)
        if dict['is-leader'] == True:
            proxyleader_ip = dict['IP']
            proxyleader_port = dict['port']

    
    topic = data["Topic"]
    # because the number of subscribers is dynamic by nature in a pubsub system, it is easier to handle subscriber 
    # information at the the broker level instead of having to possibly maintain replicas of subscribers' information at 
    # the proxy node layer
    subscribers_to_send_to = {}

    # subscribers to send to format: (unencrypted unfortunately)
    # {
    #   <sub_id>: {
    #     IP: sub['ip'],
    #     Port: sub['port']
    #   },
    #   <sub_id>: {...}
    #       
    # 
    # }
    # when proxy node gets this information:
    # for sub in subscribers_to_send_to.keys():
    #     send(decrypted message)
    print(subscriptions)
    sub_count = 0
    # as long as there are subscribers subscribed to the broker, check for any subscribers subscribed to topic
    if len(subscriptions) > 0:
      for sub in subscriptions:
        if sub['topic'] == topic:
          sub_count += 1
          if verbose: log(f"Sending message to {sub['id']} @ {sub['ip']}:{sub['port']}")
          subscribers_to_send_to[sub['id']] = {
            "ip": sub['ip'],
            "port": sub["port"]
          }

          # embed subscribers' information into message for proxy nodes to handle later
          data["Subscribers"] = subscribers_to_send_to
          log("Publisher ID: " + data["Publisher-ID"])
          log(f"data published to {topic} ({sub_count} subs)")

      # only if there are subscribers subscribed to that particular topic, then send
      if sub_count > 0:
        send_message(data, proxyleader_ip, proxyleader_port)
      else:
        log("No subscribers subscribed to " + topic)
    else:
      log("No subscribers to publish to")
#Function to set up socket between broker and proxy, and then send
#message passed as argument. The socket is TCP, not entirely sure if 
#we want to use TCP or not for this communication. 
#UDP is socket.SOCK_DGRAM instead of socket.SOCK_STREAM
def send_message(message, ip, port):
  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    # Setup socket and connect
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #This is binding the port from proxy to broker.
    #s.bind((ip, port))

    #Connect to the target's port and IP address
    connected = False
    while not connected:
      try:
        s.connect((ip, port))
        connected = True
      except:
        log("Error on connection. Retrying in 30 seconds...")
        sleep(30)

    # Send message
    if(type(message) is dict):
      message = json.dumps(message)
      s.sendall(message.encode() + EOT_CHAR)
    else:
      s.sendall(message + EOT_CHAR)
    
    # Wait for OK response
    # return s.recv(BUFFER_SIZE).decode()

#Thread function for one broker to handle multiple publisher messages at the same
#time. Sets up a socket with a single port that all publishers are sending messages 
#through, goes through each socket connection, takes all data, and then sends it 
#to the handle_pub_message() function.
def pubthread():
  log(f"Publisher thread is up at port {pub_port}")

  while True:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
      # Setup socket and listen for connections
      s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      s.bind((host, pub_port))
      s.listen()
      # Accept connections
      conn, addr = s.accept()
      data = b""
      with conn:
        if verbose: log(f"Publisher connected from {addr[0]}:{addr[1]}")
        # Loop through connections until we get the EOT_CHAR (end-of-transmission)
        while True:
          data += conn.recv(BUFFER_SIZE)
          if len(data) < 1 or data[-1] == EOT_CHAR[0]:
            data = data[:-1] # just in case any messages were sent with EOT
            break

        # Detect get_proxy_nodes header
        convertData = json.loads(data.decode())
        if 'getProxyNodes' in convertData:
          log("HANDLING STORING PROXY NODES")
          # send the proxy.json file to publisher
          with open("proxy.json", "r") as infile:
            data = infile.read()
            conn.sendall(data.encode("UTF-8"))
            infile.close()
        elif 'public-key' in convertData:
          log("HANDLING PUBLISHER'S PUBLIC KEY")
          # pass the public key to the proxy node leader
          handle_pub_ID_publickey(data)
        else:
          log("HANDLING PUBLISH MESSAGE")
          handle_pub_message(convertData)
          # this is a normal message 

#Function that handles sending publisher messages that contain publisher ID's
#and publisher public keys. 
def handle_pub_ID_publickey(data):
  global proxyleader_ip
  global proxyleader_port

  with open("proxy.json", "r") as infile:
    for line in infile:
      dict = json.loads(line)
      if dict['is-leader'] == True:
        proxyleader_ip = dict['IP']
        proxyleader_port = dict['port']
        send_message(data, proxyleader_ip, proxyleader_port)


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
  print("DATA RECEIVED FROM SUBSCRIBER: " + str(data))
  sub_id = data[0]
  action = data[1]
  topic = data[2]
  sub_port = data[3]
  logging_output = "subscribed to" if action == "sub" else "unsubscribed from"
  log(f"{sub_id} {logging_output} {topic}")
  if action == "sub":
    subscribe(sub_id, topic, addr[0], sub_port)
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
      log("HANDLING SUBSCRIBER")
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
  global proxyleader_ip
  global proxyleader_port
  
  while True:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
      # Setup socket and listen for connections, set a timeout to detect when no new proxy nodes are being added
      s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      s.bind((host, proxy_port))
      s.listen()
      s.settimeout(15)

      try:
        # Accept connections from new proxy nodes
        conn, addr = s.accept()
        s.settimeout(None)
        data = b""
        with conn:
          if verbose: log(f"Proxy node connected from {addr[0]}:{addr[1]}")
          # Loop through connections until we get the EOT_CHAR (end-of-transmission)
          while True:
            data += conn.recv(BUFFER_SIZE)
            if data[-1] == EOT_CHAR[0]:
              data = data[:-1]
              break
          
        # Write/append rows of JSON to the broker's replica of proxy.json
        if exists("proxy.json"):
          with open("proxy.json", "a") as file:
            file.write("\n")
            json.dump(json.loads(data.decode("UTF_8")), file)
        else:
          with open("proxy.json", "w") as file:
            json.dump(json.loads(data.decode("UTF-8")), file)
      except socket.timeout:
        # Begin leader election if no new proxy node connections have been established in the timeout period
        start_time = time.time()
        proxyleader_ID = -1
        proxyleader_ip = None
        proxyleader_port = None
        
        # Find proxy with the highest ID to be selected as to-be leader
        with open("proxy.json", "r") as infile:
          for line in infile:
              dict = json.loads(line)
              if dict['ID'] > proxyleader_ID:
                proxyleader_ID = dict['ID']
                proxyleader_ip = dict['IP']
                proxyleader_port = dict['port']

        # Update flag to mark highest-id proxy leader as leader
        with open("proxy.json", "r+") as infile:
          data = infile.readlines()
          for i in range(len(data)):
            dict = json.loads(data[i])
            if dict['ID'] == proxyleader_ID:
                dict['is-leader'] = True
                data[i] = dict
          infile.seek(0)
          for line in data:
            json.dump(line, infile)
          infile.truncate()

          # Send election message with proxy.json file to new proxy leader
          dictionary = {
            "election-message": True,
            "proxy-list": infile.read()
          }
          send_message(dictionary, proxyleader_ip, proxyleader_port)
        end_time = time.time()
        print("Time to identify to-be proxy leader and start leader election: ", round(start_time-end_time, 4), sep="")
        break
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