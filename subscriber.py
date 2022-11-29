import socket
from time import sleep
from sys import argv, exit
import threading

id = None
client_ip = "127.0.0.1"
client_port = None
server_ip = None
server_port = None
command_file = None
port_offset = 1
verbose = False

EOT_CHAR = b"\4"
BUFFER_SIZE = 1024


def log(message):
  print(f"[Sub {id}] " + message);

def send_message(message):
  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    # Setup socket and connect
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((client_ip, client_port))
    connected = False
    while not connected:
      try:
        s.connect((server_ip, server_port))
        connected = True
      except:
        log("Error on connection. Retrying in 30 seconds...")
        sleep(30)

    # Send message
    message = bytes(message, 'UTF-8')
    s.sendall(message + EOT_CHAR)

    # Wait for OK response
    return s.recv(BUFFER_SIZE).decode()

def subscribe(topic):
  log(f"Subscribing to {topic}")
  response = send_message(id + " sub " + topic)
  if verbose: log(f"Received {response}")

def unsubscribe(topic):
  log(f"Unsubscribing from {topic}")
  response = send_message(id + " unsub " + topic)
  if verbose: log(f"Received {response}")

def check_command(command):
  return not command[0].isdigit() or int(command[0]) < 0 or len(command) != 3 or (command[1] != "sub" and command[1] != "unsub")

def handle_command(command):
  topic = command[2]
  if(int(command[0]) > 0):
    if verbose: log(f"Waiting {command[0]} second(s)...")
    sleep(int(command[0]))
  subscribe(topic) if command[1] == "sub" else unsubscribe(topic)

def handle_command_file():
  file = open(command_file, "r").readlines()
  for command in file:
    command = command.replace("\n", "")
    if verbose: log(f"Running command from file: \"{command}\"")
    command = command.split(" ")
    handle_command(command)

def handle_cli_commands():
  try:
    while True:
      log("Enter command:")
      command = input().split(" ")
      while check_command(command):
        log("Invalid command")
        log("Use: <wait time> <sub/unsub> <topic>")
        command = input().split(" ")
      handle_command(command)
  except:
    return

def handle_option_id(arguments, i):
  global id
  id = arguments[i+1]

def handle_option_client_port(arguments, i):
  global client_port
  try:
    client_port = int(arguments[i+1])
  except: 
    print("Invalid port number")
    return -1

def handle_option_server_ip(arguments, i):
  global server_ip
  server_ip = arguments[i+1]

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

def handle_command_line_args():

  options = {
    "-i": handle_option_id,
    "-r": handle_option_client_port,
    "-h": handle_option_server_ip,
    "-p": handle_option_server_port,
    "-f": handle_option_command_file,
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

  if not id or not client_port or not server_ip or not server_port:
    print("Arguments missing")
    return -1

  return 0

def sender():
  if command_file:
    handle_command_file()
  handle_cli_commands()

def receiver():
  while True:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
      # Setup socket and listen for connections
      s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      s.bind((client_ip, client_port + port_offset))
      s.listen()

      # Accept connections
      conn, addr = s.accept()
      data = b""
      with conn:
        # Loop through connections until we get the EOT_CHAR (end-of-transmission)
        while True:
          data += conn.recv(BUFFER_SIZE)
          if data[-1] == EOT_CHAR[0]:
            data = data[:-1]
            break
        # Send OK response
        # conn.sendall(b"OK")
      log(f"Received message: {data.decode()}")

ret_val = handle_command_line_args()
if ret_val != -1:
  log("Subscriber process started")
  try:
    threading.Thread(target=sender).start()
    threading.Thread(target=receiver).start()
  except KeyboardInterrupt:
    exit(0)
else:
  print("Use: python subscriber.py -i ID -r sub_port -h broker_IP -p port [-f command_file -o port_offset -v]")