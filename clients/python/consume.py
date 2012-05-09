#!/usr/bin/env python3

import jafka

consumer = jafka.Consumer('localhost',9092)
offsets = consumer.getoffsetsbefore('demo',0,-2,100)
print('offsets',offsets)

ms = consumer.fetch('demo',0,offsets[0],1024)
for offset,message in ms:
    print(offset,message.decode('utf-8'))
