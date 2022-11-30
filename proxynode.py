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

#This function generates a new entry for the JSON file that we eventually have to send to publishers.
#We create 
def generate_JSON_File():
  (publicKey, privateKey) = rsa.newkeys(1024)
  dictionary = {
    "IP": proxy_node_ip,
    "port": proxy_node_port,
    "ID": id,
    "public-key": publicKey,
    "is-leader": False,
    "is-live": True
}

  return privateKey, publicKey

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









#Thread function for a proxy node to handle multiple broker messages at the same
#time. Sets up a socket with a single port that all brokers are sending messages 
#through, goes through each socket connection, takes all data, and then sends it 
#to the handle_broker_message() function.
def listen_broker_thread():
  log(f"Broker thread is up at port {broker_port}")

  while True:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
      # Setup socket and listen for connections
      s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      s.bind((broker_host_ip, broker_port))
      s.listen()

      # Accept connections
      conn, addr = s.accept()
      data = b""
      with conn:
        if verbose: log(f"Publisher connected from {addr[0]}:{addr[1]}")
        # Loop through connections until we get the EOT_CHAR (end-of-transmission)
        while True:
          data += conn.recv(BUFFER_SIZE)
          if data[-1] == EOT_CHAR[0]:
            data = data[:-1]
            break
        # Send OK response
        conn.sendall(b"OK")
      handle_broker_message(data)

#First we have to decrypt the message before we can take it apart, also we somehow need the broker's information
#about the subscriber 
def handle_broker_message(data):
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


#This function will listen for the public keys that are sent by each publisher, but first we need to know 
#what port and IP addresses are associated with the subscriber's first so we can listen for them.
def listen_publisher_thread():
