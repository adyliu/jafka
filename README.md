#A fast Message-Queue base on zookeeper.

Jafka is a distributed publish-subscribe messaging system cloning from Apache Kafka.

So it has following features:

* Persistent messaging with O(1) disk structures that provide constant time performance even with many TB of stored messages.
* High-throughput: even with very modest hardware single broker can support hundreds of thousands of messages per second.
* Explicit support for partitioning messages over broker servers and distributing consumption over a cluster of consumer machines while maintaining per-partition ordering semantics.
* Simple message format for many language clients.

##[Quick Start]