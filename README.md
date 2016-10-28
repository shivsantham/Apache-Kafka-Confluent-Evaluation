# Apache-Kafka-Confluent-Evaluation
Evaluating the kafka producer and consumer throughput

This is basically a throughput test for the kafka producers and consumers.

Something.json -- has static characters. Using this to build to 10k message size when sending messages through producer.

Producer:

ProducerGroup -- Launches a executor service to create a pool of producerThreads which sends "String" messages to kafka broker.
ProducerThread -- Runnable Thread which actually sends messages(10K size) to the Kafka broker.
ZookeeperUtil -- Has some utilities to create/delete/lists topics in the kafka cluster.

Consumer:

ConsumerGroup -- Launches a executor service to create a pool of Consumer threads which consumes messages from Kafka Broker.
ConsumerThread -- Runnable Thread which actually consumes messages from the Kafka broker.

Consumer is always kept open to receive messsages. Each and every consumer thread is run indefinitely by setting the ConsumerTimeOut value to the Integer_MAX and kept running in a indefinite poll loop. 

Used CyclicBarrier to control the threads for metrics collection. Once all the consumer threads are done doing their jobs, they reach the barrier point and the metrics collection is done as part of the Barrier event.

Throughput:

Hardware: Mac Os, 15Gb RAM, ~400gb used/ 512gb disk space, 7 cpu cores
Producer -- Around 120k - 140K messages per second. When sending 2 million messages per topic with message size (10k)
Consumer -- 1.2 Million - 2 Million messages per second.

I was able to see similar throughput in a Multinode cluster in a VM setup.
