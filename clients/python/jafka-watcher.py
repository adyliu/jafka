#!/usr/bin/env python3
#-*- coding:utf-8 -*-
#Jafka watcher
#author:imxylz@gmail.com
#version:0.3
#date:2012/7/19

import zookeeper
import threading
import sys
import datetime
import json
import jafka


default_host="10.11.5.145:2181/suc/jafka"
ZNODE_ACL = [{"perms":31,"scheme":"world","id":"anyone"}]

init_flag = False
handle = -1
connected = False

def init_client(host=default_host):
    global init_flag
    global handle
    global connected

    if init_flag: return False
    init_flag = True

    connected = False
    cond = threading.Condition()
    def connection_watcher(handle,type,stat,path):
        global connected
        with cond:
            connected = True
            cond.notify()
    with cond:
        zookeeper.set_debug_level(2)
        handle = zookeeper.init(host,connection_watcher)
        cond.wait(30.0)

    if not connected:
        raise Exception("Couldn't connect to host -",host)
    return True

def normal_path(path,env=""):
    if env: path = env+path
    if path[-1] == '/':
        path = path[0:-1]
    return path

def gets(path):
    init_client()
    global handle
    try:
        (data,stat) = zookeeper.get(handle,path,None)
        return (data,stat)
    except zookeeper.NoNodeException:
        return None

def get(path):
    r = gets(path)
    return r[0] if r else None

def close():
    global handle
    global init_flag
    global connected
    if not init_flag: return
    zookeeper.close(handle)
    init_flag = False
    connected = False


def exists(path):
    init_client()
    global handle
    try:
        return zookeeper.exists(handle,path) is not None
    except zookeeper.NoNodeException:
        return False

def get_children(path):
    init_client()
    global handle
    return zookeeper.get_children(handle,path)

def get_topics():
    return get_children('/brokers/topics')


def main(host=default_host):
    init_client(host)
    topics = get_children('/brokers/topics')
    brokerids = get_children('/brokers/ids')
    brokers = dict((brokerid,get('/brokers/ids/'+brokerid)) for brokerid in brokerids)
    #brokers: brokerid => (host,port)
    brokers = dict((brokerid,(v.split(':')[1],int(v.split(':')[2]))) for brokerid,v in brokers.items())

    #topic_broker_parts: topic=>((brokerid,parts),(brokerid,parts)...)
    topic_broker_parts = {}
    for topic in topics:
        topicbrokers = get_children('/brokers/topics/'+topic)
        broker_parts = []
        for b in topicbrokers:
            parts = get('/brokers/topics/'+topic+'/'+b)
            broker_parts.append((int(b),int(parts)))
        topic_broker_parts[topic] = broker_parts

    groups = get_children('/consumers')

    for group in groups:
        cids = get_children('/consumers/%s/ids'%group)
        ccounts = {}
        for cid in cids:
            topic_counts = get('/consumers/%s/ids/%s'%(group,cid))
            topic_count_map = json.loads(topic_counts)
            ccounts[cid] = topic_count_map
        ctopics = get_children('/consumers/%s/offsets'%group)

        #records: [(topic,broker,part,coffset,toffset,consumerid,lastmtime),...]
        records = []
        broker_records = {}
        for ctopic in ctopics:
            cparts = get_children('/consumers/%s/offsets/%s'%(group,ctopic))
            for cpart in cparts:
                coffset,coffsetstats = gets('/consumers/%s/offsets/%s/%s'%(group,ctopic,cpart))
                consumerid = get('/consumers/%s/owners/%s/%s'%(group,ctopic,cpart))
                consumerid = consumerid if consumerid else '-'
                #print('%15s: %20s %s => %13s'%(group,ctopic,cpart,coffset))
                cbroker,cpartition = cpart.split('-')
                lastmtime = coffsetstats['mtime'] if coffsetstats else -1
                if lastmtime:
                    lastmtime = datetime.datetime.fromtimestamp(int(lastmtime)/1000).strftime('%m-%d %H:%M:%S')
                record = [ctopic,cbroker,cpartition,coffset,-1,consumerid,lastmtime]
                ######################
                rds = broker_records.get(cbroker,[])
                if not rds: broker_records[cbroker] = rds
                rds.append(record)
                records.append(record)

        for broker,rds in broker_records.items():
            (host,port) = brokers[str(broker)]
            consumer = jafka.Consumer(host,port)
            try:
                for record in rds:
                    toffset = consumer.getoffsetsbefore(record[0],int(record[2]),-1,1)[0]
                    record[4] = toffset
            finally:
                consumer.close()

        title=('groupid','topic','part','consumeoffset','totaloffset','backlog','consumerid','lastmtime')
        wid_sep = list(len(x) for x in title)
        records=sorted(records,key=lambda r:r[0]+r[1]+r[2])
        print_records=[]
        for record in records:
            (ctopic,cbroker,cpartition,coffset,toffset,consumerid,lastmtime) = record
            left = int(toffset) - int(coffset)
            pr = (group,ctopic,cbroker+'-'+cpartition,coffset,toffset,left,consumerid,lastmtime)
            print_records.append(pr)
            wid_sep_num = list(len(str(x)) for x in pr)
            for i in range(len(wid_sep)):
                if wid_sep[i] < wid_sep_num[i]:
                    wid_sep[i] = wid_sep_num[i]
        format_sep=' '.join(list('{:>'+str(x)+'}' for x in wid_sep))
        ptitle = format_sep.format(*title)
        print(ptitle)
        print('-'*len(ptitle))
        for pr in print_records:
            print(format_sep.format(*pr))
        print()
    

if __name__ == '__main__':
    print('Jafka watcher v0.2')
    print()
    try:
        main()
    finally:
        close()
    


