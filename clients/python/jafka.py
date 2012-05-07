#!/usr/bin/env python3

import socket
import struct
import binascii
import sys

PRODUCE_TYPE=0

def encode_message(message):
    if type(message) == str:
        message = message.encode('utf-8')

    print(~(binascii.crc32(message)^0xffffffff))
    return struct.pack('>B',1)+\
           struct.pack('>B',0)+\
           struct.pack('>I',(binascii.crc32(message) & 0xffffffff))+\
           message

def encode_produce_request(topic,partition,messages):
    if type(topic) == str: topic = topic.encode('utf-8')
    encoded = [encode_message(message) for message in messages]
    print('encoded->',encoded)
    message_set = b''.join([struct.pack('>i',len(m))+m for m in encoded])
    print(message_set)

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
        print('Usage: %s <topic> [host [port]]'%(sys.argv[0]))
        sys.exit(1)
    topic = sys.argv[1]
    host = argv[2] if len(sys.argv)>2 else 'localhost'
    port = int(argv[3]) if len(sys.argv)>3 else 9092

    producer = Producer(host,port)

    while True:
        print('Enter a message: ')
        line = sys.stdin.readline()
        if not line or not line.strip():
            break
        print('send->',(line,))
        producer.send(topic,(line,))
        

