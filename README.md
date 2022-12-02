# COEN317PubSub

## Introduction 
- As part of our COEN 317: Distributed Systems final project, we aim to address critical security concerns in the publish subscribe messaging architecture. This project implements a variation of a publish-subscribe system that utilizes RSA encryption and digital signatures within the publisher layer, and a scalable security layer composed of proxy nodes to distribute security workloads away from the message broker and subscriber. All functionality is coded in Python.

## Requirements 
- You should have at least Python 3.8+ to ensure that the most recent libraries can be installed without an issue. 

## Installation 
- To create your python virtual environment, use the command `python3 venv -m <environment name>`
- Use the following command to install the necessary module PyCryptodome: `pip3 install pycryptodome`

## Configuration/Runtime
- To run this publish-subscribe system, there is a general order in which to instantiate the entities: broker --> proxy node --> subscriber --> publisher 
- For functionality to work as intended, the broker MUST be run on a separate machine while the other files can be run locally on another machine. For each command that is run, you will want to have a separate terminal window open. For starters, you may run 1 broker, 2 publishers, 3 proxy nodes, and 4 subscribers. You may increase the number of any of the entities except the broker.
- To run the broker, you may use the following command:
  - python3 broker.py -s 8001 -p 8000 -pr 8002 -o 1 -v
- To run the proxy nodes, you may run any number of the following commands. However, for each proxy node, change the port AND identifier used to avoid port/identifier collision (-p): 
  - python3 proxynode.py -i 1 -b {broker-ip} -br 8002 -ip 127.0.0.1 -p 9000
  - python3 proxynode.py -i 2 -b {broker-ip} -br 8002 -ip 127.0.0.1 -p 9001
  - and so forth...
- To run the subscribers, you may run any number of the following commands. However, for each subscriber node, 
  - python3 subscriber.py -i s1 -r 7000 -h {broker-ip} -p 8002 -o 1 -v
  - python3 subscriber.py -i s2 -r 7002 -h {broker-ip} -p 8002 -o 1 -v
  - python3 subscriber.py -i s3 -r 7004 -h {broker-ip} -p 8002 -o 1 -v
  - and so forth...
- To run the publishers, you may use the following command. Similar to the proxy nodes, use different port numbers AND identifiers to avoid port/identifier collisions
  - python3 publisher.py -r 8010 -i p1 -h {broker-ip} -p 8000 -v
  - python3 publisher.py -r 8010 -i p2 -h {broker-ip} -p 8000 -v
  - and so forth...

## Publisher and Subscriber Commands + Command Files
- Once the publishers and subscribers are created, they each have their own sets of commands. To see message traffic, subscribers' commands should go before publishers' commands go so they are ready to receive messages. After that, you can see how the subscribers subscribe/unsubscribe and receive messages while publishers are publishing messages
- Publisher --> to publish messages to a given topic, use the following command format for each running publisher: `<wait time> pub #<topic> <message>`
  - 1 pub #sports message from p1
  - 1 pub #testtopic message from p2
  - NOTE: the publishers should publish to topics that the subscribers are already subscribed to
- Subscriber --> the subscriber can either subscribe or unsubscribe from a topic in the broker with the following formats
  - 1 sub #sports
  - 1 sub #testtopic
  - 10 unsub #sports
  - 10 sub #sports
  - 10 unsub #testtopic
  - 10 sub #testtopic
- Command files 
  - Both publishers and subscribers can have command files (.txt) that are a list of corresponding commands (1 command per line). To add them at runtime, simply add the `-f <command file name>` to the runtime terminal commands when creating the publisher(s) and subscriber(s)


