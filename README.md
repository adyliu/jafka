#A fast distributed messaging system (MQ)

Jafka mq is a distributed publish-subscribe messaging system cloning from [Apache Kafka](http://incubator.apache.org/kafka/).

So it has following features:

* Persistent messaging with O(1) disk structures that provide constant time performance even with many TB of stored messages.
* High-throughput: even with very modest hardware single broker can support hundreds of thousands of messages per second.
* Explicit support for partitioning messages over broker servers and distributing consumption over a cluster of consumer machines while maintaining per-partition ordering semantics.
* Simple message format for many language clients.

If you are interesting in [scala](http://www.scala-lang.org/), please using the origin kafka at [apache](http://incubator.apache.org/kafka/). Also it has git repository at [github](https://github.com/apache/kafka/).

## News

[2012-06-25] [released](https://github.com/adyliu/jafka/wiki/history) [v1.2.0](http://repo1.maven.org/maven2/com/sohu/jafka/jafka/)

##Document & Wiki

Wiki: [https://github.com/adyliu/jafka/wiki](https://github.com/adyliu/jafka/wiki)

----
[Keywords: jafka, kafka, messaging system, mq, jafka mq, sohu]
