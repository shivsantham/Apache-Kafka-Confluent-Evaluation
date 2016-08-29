# Apache-Kafka-Confluent-Evaluation
Evaluating the kafka producer and consumer throughput

This is basically a throughput test for the kafka producers and consumers.

Producer:

ProducerGroup -- Launches a executor service to create a pool of producerThreads which sends "String" messages to kafka broker.
ProducerThread -- Runnable Thread which actually sends messages to the Kafka broker.
ZookeeperUtil -- Has some utilities to create/delete/lists topics in the kafka cluster.

Consumer:

ConsumerGroup -- Launches a executor service to create a pool of Consumer threads which consumes messages from Kafka Broker.
ConsumerLogic -- Runnable Thread which actually consumes messages from the Kafka broker.

Consumer is always kept open to receive messsages. Each and every consumer thread is run definitely by setting the ConsumerTimeOut value to the Integer_MAX and kept running in a indefinite poll loop. 

Used CycliBarrier to control the threads for metrics collection. Once all the consumer threads are done doing their jobs, they reach the barrier point and the metrics collection is done as part of the Barrier event.






