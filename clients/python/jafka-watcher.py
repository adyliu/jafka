#!/usr/bin/env python3
#-*- coding:utf-8 -*-
#Jafka watcher
#author:imxylz@gmail.com
#version:0.3
#date:2012/7/19

import kazoo
import kazoo.client
import kazoo.exceptions
import threading
import sys
import datetime
import json
import jafka
import os


ZNODE_ACL = [{"perms":31,"scheme":"world","id":"anyone"}]


class _zk:
    def __init__(self, hosts: str = None, **kwargs):
        self.hosts = hosts or os.getenv('ZK_HOSTS', '192.168.6.22:2181')+os.getenv('JAFKA_PATH', '/xpower/jafka')
        self.client = kazoo.client.KazooClient(hosts=self.hosts, **kwargs)
        self.started = False

    def start(self):
        if not self.started:
            self.started = True
            self.client.start()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.stop()
        self.client.close()
        self.started = False

    def ensure(self, path):
        return self.client.ensure_path(path)

    def create(self, path, data):
        data = data.encode('utf-8') if isinstance(data, str) else data
        return self.client.create(path, data)

    def set(self, path, data):
        data = data.encode('utf-8') if isinstance(data, str) else data
        try:
            return self.client.set(path, data)
        except kazoo.exceptions.NoNodeError:
            return None

    def delete(self, path):
        try:
            return self.client.delete(path)
        except kazoo.exceptions.NoNodeError:
            return None

    def rdelete(self, path):
        if path == '/': raise Exception('delete / is forbidden')
        return self.client.delete(path, recursive=True)

    def gets(self, path):
        try:
            data,stat = self.client.get(path)
            data = data.decode('utf8')
            return (data,stat)
        except kazoo.exceptions.NoNodeError:
            return None
    def get(self, path):
        r = self.gets(path)
        return None if r is None else r[0]

    def list(self, path):
        try:
            return self.client.get_children(path)
        except kazoo.exceptions.NoNodeError:
            return None


def main(zk):
    topics = zk.list('/brokers/topics')
    brokerids = zk.list('/brokers/ids')
    brokers = dict((brokerid,zk.get('/brokers/ids/'+brokerid)) for brokerid in brokerids)
    #brokers: brokerid => (host,port)
    brokers = dict((brokerid,(v.split(':')[1],int(v.split(':')[2]))) for brokerid,v in brokers.items())

    #topic_broker_parts: topic=>((brokerid,parts),(brokerid,parts)...)
    topic_broker_parts = {}
    for topic in topics:
        topicbrokers = zk.list('/brokers/topics/'+topic)
        broker_parts = []
        for b in topicbrokers:
            parts = zk.get('/brokers/topics/'+topic+'/'+b)
            broker_parts.append((int(b),int(parts)))
        topic_broker_parts[topic] = broker_parts

    groups = zk.list('/consumers')
    #print('groups=',groups)

    for group in groups:
        cids = zk.list('/consumers/%s/ids'%group)
        ccounts = {}
        for cid in cids:
            topic_counts = zk.get('/consumers/%s/ids/%s'%(group,cid))
            topic_count_map = json.loads(topic_counts)
            ccounts[cid] = topic_count_map
        ctopics = zk.list('/consumers/%s/offsets'%group) or []

        #records: [(topic,broker,part,coffset,toffset,consumerid,lastmtime),...]
        records = []
        broker_records = {}
        for ctopic in ctopics:
            cparts = zk.list('/consumers/%s/offsets/%s'%(group,ctopic))
            for cpart in cparts:
                coffset,coffsetstats = zk.gets('/consumers/%s/offsets/%s/%s'%(group,ctopic,cpart))
                #print('coffsetstats=',coffsetstats)
                consumerid = zk.get('/consumers/%s/owners/%s/%s'%(group,ctopic,cpart))
                consumerid = consumerid if consumerid else '-'
                #print('%15s: %20s %s => %13s'%(group,ctopic,cpart,coffset))
                cbroker,cpartition = cpart.split('-')
                lastmtime = coffsetstats.mtime if coffsetstats else -1
                if lastmtime:
                    lastmtime = datetime.datetime.fromtimestamp(int(lastmtime)/1000).strftime('%Y-%m-%d %H:%M:%S')
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
        for record in print_records:
            #print(format_sep,' -> ',record)
            print(format_sep.format(*record))
        print('\n')


if __name__ == '__main__':
    print('Jafka watcher v0.3')
    print()
    with _zk() as zk:
        main(zk)



