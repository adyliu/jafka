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

import sys
import jafka
import time

DEFAULT_MAX_MESSAGE_SIZE = 1024*1024
out = sys.stdout

def packagesize(messagesize,batchsize,topic):
    return (10 + messagesize)*batchsize +16+len(topic.encode('utf-8'))

def check_message_size(messagesize,batchsize,topic):
    size = packagesize(messagesize,batchsize,topic)
    if size > DEFAULT_MAX_MESSAGE_SIZE:
        print('The message package(%d) is too large;default message package is: %d' %\
            (size,DEFAULT_MAX_MESSAGE_SIZE))
        print('  package size = (10 + messagesize)*batchsize +16+topic')
        dsize = DEFAULT_MAX_MESSAGE_SIZE - 16 - len(topic.encode('utf8'))
        print('  messagesize(%d): batchsize <= %.d' % (messagesize,dsize/messagesize))
        print('  batchsize(%d): messaegsize <= %.d' % (batchsize,dsize/batchsize))
        sys.exit(1)
    return size
def is_valid_messages(messagesize,batchsize,topic):
    return packagesize(messagesize,batchsize,topic) < DEFAULT_MAX_MESSAGE_SIZE

def show_progress(i,count):
    out.write('%05.2f%%'%(100*i/(1.0*count)))
    out.flush()

def clear_progress():
    out.write('\b'*6)
    out.flush()

def write_line(batchsize):
    out.write('\n')
    out.write('%5d '%(batchsize,))
    out.flush()
def write_result(tps,bytesize,seconds):
    out.write('%6.d|%04.d|%03.1f '%(tps,bytesize/(1024*1024),seconds))

def compute(producer,batchsize,messagesize,times,topic):
    psize = packagesize(messagesize,batchsize,topic)
    start = time.time()
    messages = [bytearray(messagesize) for i in range(batchsize)]
    for i in range(times):
        producer.send(topic,messages)

    seconds = time.time() - start
    tps = batchsize * times / seconds
    bytesize = psize * times
    return tps,bytesize,seconds

if __name__ == '__main__':

    (topic,host,port,count) = ('demo','localhost',9092,100000)
    producer = jafka.Producer(host,port)
    batchsizes = (1,10,50,100,200,500,1000,2000)
    messagesizes = (16,32,64,128,256,1024,2048)
    print('send %d messages to %s:%d' % (count,host,port))
    print('    batchsize(bs) = ',batchsizes)
    print('    messagesizes(ms) = ',messagesizes)
    print('    result report contains tree fields: tps|sendbytes|costtime')
    print('        tps(Total Transactions per Second)')
    print('        sendbytes(MB)')
    print('        costtime(seconds)')
    print()

    totalcount = 0
    bm = []
    for batchsize in batchsizes:
        for messagesize in messagesizes:
            if not is_valid_messages(messagesize,batchsize,topic):
                break
            times = (int)(count/batchsize)
            totalcount += times * batchsize
            bm.append((batchsize,messagesize,times))

    lastbatchsize = -1
    i = 0
    out.write('bs|ms %s' % (' '.join(['%15s'%ms for ms in messagesizes])))
    for batchsize,messagesize,times in bm:
        if lastbatchsize != batchsize: #new line 
            write_line(batchsize)
            lastbatchsize = batchsize
        show_progress(i,totalcount)
        tps,bytesize,seconds = compute(producer,batchsize,messagesize,times,topic)
        i += batchsize * times
        clear_progress()
        write_result(tps,bytesize,seconds)
    
    print()
