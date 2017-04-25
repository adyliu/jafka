#A fast distributed messaging system (MQ)

[![License](https://img.shields.io/github/license/adyliu/jafka.svg)](https://github.com/adyliu/jafka/blob/master/LICENSE)
[![Build Status](https://travis-ci.org/adyliu/jafka.png?branch=master)](https://travis-ci.org/adyliu/jafka)
[![Maven Central](https://img.shields.io/maven-central/v/io.jafka/jafka.svg)](http://search.maven.org/#search|ga|1|g%3A%22io.jafka%22%20a%3A%22jafka%22)

Jafka mq is a distributed publish-subscribe messaging system cloned from [Apache Kafka](http://kafka.apache.org/).

So it has the following features:

* Persistent messaging with O(1) disk structures that provide constant time performance even with many TB of stored messages.
* High-throughput: even with very modest hardware single broker can support hundreds of thousands of messages per second.
* Explicit support for partitioning messages over broker servers and distributing consumption over a cluster of consumer machines while maintaining per-partition ordering semantics.
* Simple message format for many language clients.
* Pure Java work

If you are interested in [scala](http://www.scala-lang.org/), please use the origin kafka at [apache](http://kafka.apache.org/). Also it has a git repository at [github](https://github.com/apache/kafka/).

## News

[2017-04-25] [released](https://github.com/adyliu/jafka/wiki/history) [v3.0.2](http://central.maven.org/maven2/io/jafka/jafka/)

## Document & Wiki

Wiki: [https://github.com/adyliu/jafka/wiki](https://github.com/adyliu/jafka/wiki)

## Download

You can download the full package from Google Drive:

* Jafka Releases [https://github.com/adyliu/jafka/releases](https://github.com/adyliu/jafka/releases)
* Google Drive [https://drive.google.com/drive/folders/0B4VObojKr49KeVNaTnc3bDlKNXM](https://drive.google.com/drive/folders/0B4VObojKr49KeVNaTnc3bDlKNXM)
* Baidu Pan [http://pan.baidu.com/s/1kU2LuwJ](http://pan.baidu.com/s/1kU2LuwJ)

## Maven & Gradle Dependencies

* [http://central.maven.org/maven2/io/jafka/jafka/](http://central.maven.org/maven2/io/jafka/jafka/)
* [http://mvnrepository.com/artifact/io.jafka/jafka](http://mvnrepository.com/artifact/io.jafka/jafka)

Maven

    <dependency>
        <groupId>io.jafka</groupId>
        <artifactId>jafka</artifactId>
        <version>3.0.2</version>
    </dependency>

Gradle

    'io.jafka:jafka:3.0.2'


## Contributor

* @rockybean
* @tiny657

## License

Apache License 2.0 => [https://github.com/adyliu/jafka/blob/master/LICENSE](https://github.com/adyliu/jafka/blob/master/LICENSE)

----
[Keywords: jafka, kafka, messaging system, mq, jafka mq, sohu]
