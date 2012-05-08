#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import socket
import struct
import binascii
import sys

PRODUCE_TYPE=0

def encode_message(message):
    if type(message) == str:
        message = message.encode('utf-8')

    return struct.pack('>B',1)+\
           struct.pack('>B',0)+\
           struct.pack('>I',(binascii.crc32(message) & 0xffffffff))+\
           message

def encode_produce_request(topic,partition,messages):
    if type(topic) == str: topic = topic.encode('utf-8')
    if type(messages) == str: messages = (messages,)
    encoded = [encode_message(message) for message in messages]
    message_set = b''.join([struct.pack('>i',len(m))+m for m in encoded])

    data = struct.pack('>H',PRODUCE_TYPE)+\
           struct.pack('>H',len(topic)) + topic+\
           struct.pack('>i',partition)+\
           struct.pack('>i',len(message_set))+message_set
    return struct.pack('>i',len(data)) + data

class Producer:
    def __init__(self,host='localhost',port=9092):
        self.connection = socket.socket()
        self.connection.connect((host,port))
    def close(self):
        self.connection.close()
    def send(self,topic,messages,partition=0):
        data = encode_produce_request(topic,partition,messages)
        self.connection.sendall(data)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: %s <topic> [host [port [test [count]]]]'%(sys.argv[0]))
        print('     : %s demo'%(sys.argv[0]))
        print('     : %s demo localhost 9092 test 100000'%(sys.argv[0]))
        sys.exit(1)
    topic = sys.argv[1]
    host = sys.argv[2] if len(sys.argv)>2 else 'localhost'
    port = int(sys.argv[3]) if len(sys.argv)>3 else 9092
    testable = len(sys.argv)>4 and sys.argv[4]=='test'
    count = int(sys.argv[5]) if len(sys.argv)>5 else 1000

    producer = Producer(host,port)

    if testable:
        import time
        start = time.time()
        for i in range(count):
            producer.send('demo','message#'+str(i))
            if i % 10000 == 0:
                print('send message count: ',i)
        end = time.time()
        print('send %d messages cost %s' % (count,(end-start)))
        sys.exit(0)
    while True:
        print('Enter a message: '),
        line = sys.stdin.readline()
        if not line or not line.strip():
            break
        producer.send(topic,(line.strip(),))
        

