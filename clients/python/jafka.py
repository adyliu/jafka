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
from struct import pack,unpack
import binascii
import sys
from time import sleep

class RequestTypes:
    PRODUCE = 0
    FETCH = 1
    MULTIFETCH = 2
    MULTIPRODUCE = 3
    OFFSETS = 4

PRODUCE_TYPE=RequestTypes.PRODUCE
FETCH_TYPE=RequestTypes.FETCH
MULTIFETCH_TYPE=RequestTypes.MULTIFETCH
MULTIPRODUCE_TYPE=RequestTypes.MULTIPRODUCE
OFFSETS_TYPE=RequestTypes.OFFSETS

def encode_message(message):
    if type(message) == str:
        message = message.encode('utf-8')

    return pack('>B',1)+\
           pack('>B',0)+\
           pack('>I',(binascii.crc32(message) & 0xffffffff))+\
           message

def encode_produce_request(topic,partition,messages):
    if type(topic) == str: topic = topic.encode('utf-8')
    if type(messages) == str: messages = (messages,)
    encoded = [encode_message(message) for message in messages]
    message_set = b''.join([pack('>i',len(m))+m for m in encoded])

    data = pack('>H',PRODUCE_TYPE)+\
           pack('>H',len(topic)) + topic+\
           pack('>i',partition)+\
           pack('>i',len(message_set))+message_set
    return pack('>i',len(data)) + data

class Producer:
    def __init__(self,host='localhost',port=9092):
        self.connection = socket.socket()
        self.connection.connect((host,port))
    def close(self):
        self.connection.close()
    def send(self,topic,messages,partition=0):
        data = encode_produce_request(topic,partition,messages)
        self.connection.sendall(data)

class OffsetRequest:
    def __init__(self,topic,partition=0,time=0,maxnumoffsets=1):
        self.topic = topic
        self.partition=partition
        self.time = time
        self.maxnumoffsets = maxnumoffsets

    def tobytes(self):
        btopic = self.topic.encode('utf-8')
        buf = pack('>H',OFFSETS_TYPE) + pack('>H',len(btopic)) + btopic + \
              pack('>i',self.partition) + pack('>q',self.time) + \
              pack('>i',self.maxnumoffsets)
        buf = pack('>i',len(buf)) + buf
        return buf

class FetchRequest:
    def __init__(self,topic,partition=0,offset=0,maxsize=1024*1024):
        self.topic = topic
        self.partition=partition
        self.offset = offset
        self.maxsize = maxsize

    def tobytes(self):
        btopic = self.topic.encode('utf-8')
        buf = pack('>H',FETCH_TYPE) + pack('>H',len(btopic)) + btopic + \
              pack('>i',self.partition) + pack('>q',self.offset) + \
              pack('>i',self.maxsize)
        buf = pack('>i',len(buf)) + buf
        return buf
class Consumer:
    def __init__(self,host='localhost',port=9092):
        self.connection = socket.socket()
        self.connection.connect((host,port))
    def close(self):
        self.connection.close()
    def getoffsetsbefore(self,topic,partition=0,time=0,maxnumoffsets=1):
        offsetRequest = OffsetRequest(topic,partition,time,maxnumoffsets)
        self.connection.sendall(offsetRequest.tobytes())
        ret = bytearray()
        while len(ret)<10:
            ret += self.connection.recv(1024)
        size = unpack('>I',ret[0:4])[0]
        remain = size + 4 - len(ret)
        while remain >0:
            r = self.connection.recv(remain)
            ret += r
            remain -= len(r)
        errorcode = unpack('>H',ret[4:6])[0]
        offsetnum = unpack('>I',ret[6:10])[0]
        offsets = [] if offsetnum ==0 else unpack('>%dq'%(offsetnum),ret[10:])
        return offsets

    def fetch(self,topic,partition,offset,maxsize=1024*1024):
        fetchRequest = FetchRequest(topic,partition,offset,maxsize)
        self.connection.sendall(fetchRequest.tobytes())
        ret = bytearray()
        while len(ret)<6:
            ret += self.connection.recv(maxsize)
#        print('ret==>',ret)
        size,errorcode = unpack('>IH',ret[0:6])
        remain = size + 4 - len(ret)
        if errorcode != 0:
            return
        if size == 6:
            return []
        while remain >0:
            r = self.connection.recv(remain)
            ret += r
            remain -= len(r)
        index = 6
        remain = size - index
        messageandoffsets = []
        offs = offset
        while remain >= 10:
            msize,version,attribute,crc32 = unpack('>IbbI',ret[index:index+10])
            index += 10
            remain -= 10
            #ignore CRC32
            if remain < msize - 6:#read over
                break
            offs = offs + 4 + msize
            messageandoffsets.append((offs,ret[index:index+msize-6]))
            index += msize - 6
            remain -= msize -6
            #print('messageandoffsets',messageandoffsets)
        return messageandoffsets
        

        

        

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: %s <topic> [host [port [test [count]]]]'%(sys.argv[0]))
        print('       %s demo'%(sys.argv[0]))
        print('       %s demo localhost 9092 test 100000'%(sys.argv[0]))
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
        

