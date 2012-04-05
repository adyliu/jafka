#Jafka Quick Start

## Step 1: Download the code

Download source: https://github.com/adyliu/jafka

    [adyliu@adyliu-work gits] git clone git@github.com:/adyliu/jafka.git
    [adyliu@adyliu-work gits] cd jafka
    [adyliu@adyliu-work jafka]$ mvn package dependency:copy-dependencies

##Step 2: Start the server
    [adyliu@adyliu-work jafka]$ sh bin/server-single.sh config/server-single.properties
    
##Step 3: Send some message
We send some messages with the console client in jafka:

    [adyliu@adyliu-work jafka]$ bin/producer-console.sh --broker-list 0:localhost:9092 --topic demo
    > Welcome to jafka
    > 中文中国

Now enter some message on the screen and exit it with ENTER or CTRL+C.

##Step 4: Start a consumer
Now we start the consumer to see these messages.

    [adyliu@adyliu-work jafka]$ bin/simple-consumer-console.sh --topic demo --server jafka://localhost:9092
    [1] 26: Welcome to jafka
    [2] 48: 中文中国
    
##Step 5: Write some code

Write a producer client:

        public static void main(String[] args) throws Exception {
            Properties props = new Properties();
            props.put("broker.list", "0:127.0.0.1:9092");
            props.put("serializer.class", StringEncoder.class.getName());
            //
            ProducerConfig config = new ProducerConfig(props);
            Producer<String, String> producer = new Producer<String, String>(config);
            //
            StringProducerData data = new StringProducerData("demo");
            for(int i=0;i<1000;i++) {
                data.add("Hello world #"+i);
            }
            //
            try {
                long start = System.currentTimeMillis();
                for (int i = 0; i < 100; i++) {
                    producer.send(data);
                }
                long cost = System.currentTimeMillis() - start;
                System.out.println("send 100000 message cost: "+cost+" ms");
            } finally {
                producer.close();
            }
        }

We get the result:
    [adyliu@adyliu-work jafka]$ bin/run-console.sh demo.client.StaticBrokerSender
    send 100000 message cost: 685 ms
    

##Step 6: Using Zookeeper

Jafka use zookeeper to auto-config brokers and consumers.

(1)First start a zookeeper server.    We use a single-node zookeeper instance for test.
    [adyliu@adyliu-work jafka]$ bin/zookeeper-server.sh config/zookeeper.properties 
    
(2)Second start the Jafka server.
    [adyliu@adyliu-work jafka]$ bin/server-single.sh config/server.properties 
    [2012-04-24 12:29:56,526] INFO Starting Jafka server... (com.sohu.jafka.server.Server.java:68)
    [2012-04-24 12:29:56,532] INFO starting log cleaner every 60000 ms (com.sohu.jafka.log.LogManager.java:155)
    [2012-04-24 12:29:56,552] INFO connecting to zookeeper: 127.0.0.1:2181 (com.sohu.jafka.server.Zookeeper.java:80)
    [2012-04-24 12:29:56,568] INFO Starting ZkClient event thread. (com.github.zkclient.ZkEventThread.java:64)

(3)Now start a producer to send some message.
    [adyliu@adyliu-work jafka]$ bin/producer-console.sh --zookeeper localhost:2181 --topic demo
    Enter you message and exit with empty string.
    > Jafka second day
    > Jafka use zookeeper to search brokers and consumers                                       
    > 

(4)It's the time to consume these messages.
    [adyliu@adyliu-work jafka]$ bin/consumer-console.sh --zookeeper localhost:2181 --topic demo --from-beginning
    Jafka second day
    Jafka use zookeeper to search brokers and consumers
    
Maybe you want to start many brokers/producers/consumers. OK, just do it. (Tips: brokerid in broker config must be unique)

(5)Write some code.

Producer code:
       public static void main(String[] args) throws Exception {
            Properties props = new Properties();
            props.put("zk.connect", "localhost:2181");
            props.put("serializer.class", StringEncoder.class.getName());
            //
            ProducerConfig config = new ProducerConfig(props);
            Producer<String, String> producer = new Producer<String, String>(config);
            //
            StringProducerData data = new StringProducerData("demo");
            for(int i=0;i<100;i++) {
                data.add("Hello world #"+i);
            }
            //
            try {
                long start = System.currentTimeMillis();
                for (int i = 0; i < 100; i++) {
                    producer.send(data);
                }
                long cost = System.currentTimeMillis() - start;
                System.out.println("send 10000 message cost: "+cost+" ms");
            } finally {
                producer.close();
            }
        }
        
Consumer Code:
        public static void main(String[] args) throws Exception {
    
            Properties props = new Properties();
            props.put("zk.connect", "localhost:2181");
            props.put("groupid", "test_group");
            //
            ConsumerConfig consumerConfig = new ConsumerConfig(props);
            ConsumerConnector connector = Consumer.create(consumerConfig);
            //
            Map<String, List<MessageStream<String>>> topicMessageStreams = connector.createMessageStreams(ImmutableMap.of("demo", 2), new StringDecoder());
            List<MessageStream<String>> streams = topicMessageStreams.get("demo");
            //
            ExecutorService executor = Executors.newFixedThreadPool(2);
            final AtomicInteger count = new AtomicInteger();
            for (final MessageStream<String> stream : streams) {
                executor.submit(new Runnable() {
    
                    public void run() {
                        for (String message : stream) {
                            System.out.println(count.incrementAndGet() + " => " + message);
                        }
                    }
                });
            }
            //
            executor.awaitTermination(1, TimeUnit.HOURS);
        } 

It's some simple, right?
Just have a try!        