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

if __name__ == '__main__':
    argv=sys.argv
    argvs=len(argv)
    if len(sys.argv) < 5:
        print('Usage: %s <topic> <host> <port> <count> [batchsize [messagesize]]'%(argv[0]))
        print('     : %s demo localhost 9022 10000'%(sys.argv[0]))
        print('     : %s demo localhost 9022 10000 100 100'%(sys.argv[0]))
        sys.exit(1)
    (topic,host,port,count) = (argv[1],argv[2],int(argv[3]),int(argv[4]))
    batchsize   = int(argv[5]) if argvs>5 else 1
    messagesize = int(argv[6]) if argvs>6 else 100

    package_size = check_message_size(messagesize,batchsize,topic)

    print('send %d messages to topic[%s] at %s:%d\n  batch size=%d\n  message size: %d' % (count,topic,host,port,batchsize,messagesize))
    producer = jafka.Producer(host,port)

    start = time.time()
    i = 0
    send_bytes = 0
    messages = [bytearray(messagesize) for i in range(batchsize)]
    out=sys.stdout
    clear='\b'*6
    while i < count:
        producer.send('demo',messages)
        i += batchsize
        if i > batchsize:
            out.write(clear)
        out.write('%05.2f%%'%(100*i/(1.0*count)))
        out.flush
        send_bytes += package_size

    print()
    end = time.time()
    print('send %d messages cost %.3f seconds' % (i,(end-start)))
    print('send %d(%.2fM) bytes message'%(send_bytes,send_bytes/(1024*1024)))
    print('tps(Total Transactions per Second): %.d' % (i/(end-start)))


