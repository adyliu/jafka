#A fast distributed messaging system (MQ)

[![Build Status](https://travis-ci.org/adyliu/jafka.png?branch=master)](https://travis-ci.org/adyliu/jafka)

Jafka mq is a distributed publish-subscribe messaging system cloned from [Apache Kafka](http://kafka.apache.org/).

So it has the following features:

* Persistent messaging with O(1) disk structures that provide constant time performance even with many TB of stored messages.
* High-throughput: even with very modest hardware single broker can support hundreds of thousands of messages per second.
* Explicit support for partitioning messages over broker servers and distributing consumption over a cluster of consumer machines while maintaining per-partition ordering semantics.
* Simple message format for many language clients.
* Pure Java work

If you are interested in [scala](http://www.scala-lang.org/), please use the origin kafka at [apache](http://kafka.apache.org/). Also it has a git repository at [github](https://github.com/apache/kafka/).

## News

[2014-11-13] [released](https://github.com/adyliu/jafka/wiki/history) [v1.4.0](http://repo1.maven.org/maven2/com/sohu/jafka/jafka/)

## Document & Wiki

Wiki: [https://github.com/adyliu/jafka/wiki](https://github.com/adyliu/jafka/wiki)

## Download

You can download the full package from Google Drive:

* Google Drive [https://googledrive.com/host/0B4VObojKr49KeVNaTnc3bDlKNXM/](https://googledrive.com/host/0B4VObojKr49KeVNaTnc3bDlKNXM/)

## Contributor

* @rockybean
* @tiny657

## License

Apache License 2.0 => [https://github.com/adyliu/jafka/blob/master/LICENSE](https://github.com/adyliu/jafka/blob/master/LICENSE)

----
[Keywords: jafka, kafka, messaging system, mq, jafka mq, sohu]
