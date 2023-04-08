package io.confluent.examples.producer;


import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

public class ProducerGroup {
    private ExecutorService executor;
    private static int DEFAULT_NO_THREADS = 10;
    private static int DEFAULT_SLEEP_TIME = 5000;
    private int noOfPartition = 5;
    private int replicationFactor = 1;
    public static String BootStrapServer = "http://localhost:29092";
    public static long totalTimeProducing;
    static Object lock = new Object();
    public String filePath = "/home/nam/work/Apache-Kafka-Confluent-Evaluation/kafka-clients/producer/src/main/resources/test.json";
    private String[] topicList = null;

    ProducerGroup(int noOfThreads, int noOfMessages, String[] topicList) {
        this.topicList = topicList;
        ArrayList<String> topics = new ArrayList<String>(Arrays.asList(topicList));
        // https://stackoverflow.com/questions/65566929/cant-import-zkstringserializer
        // ZookeeperUtil.createTopics(topics, noOfPartition, replicationFactor);
        try (AdminClient client = AdminClient.create(this.producerConfig())) {
            boolean topicExists = client.listTopics().names().get().stream().anyMatch(topicName -> topicName.equalsIgnoreCase("test-topic"));
            if (topicExists) {
                return;
            }
            CreateTopicsResult result = client.createTopics(Arrays.asList(
                    new NewTopic("test-topic", 1, (short) 1)
                    // new NewTopic("global-id-topic", 1, (short) 1),
                    // new NewTopic("snapshot-topic", 1, (short) 1)
            ));
            result.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            e.printStackTrace();
            throw new IllegalStateException(e);
        }
    }

    private Properties producerConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BootStrapServer);
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }

    public void run(int numThreads) {
        // executor quản lý thread
        executor = Executors.newFixedThreadPool(numThreads);
        int keyNo = 0;

        // prepare data
        String toReturn = null;
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = null;
        try {
            Object obj = parser.parse(new FileReader(filePath));
            jsonObject = (JSONObject) obj;
        } catch (Exception e) {
            e.printStackTrace();
        }

        // tạo 5 thread
        // mỗi thread đại diện 1 topic
        for (int i = 0; i < numThreads; i++) {
            keyNo = i % 5; // 5 can be replaced with number of topics.
            executor.submit(new Thread() {
                public void run() {
                    // keyNo = i % 5;
                    int min = 1;
                    int max = 5;
                    int keyNo = ThreadLocalRandom.current().nextInt(min, max + 1);
                    Producer<String, String> producer = new KafkaProducer<String, String>(producerConfig());
                    try {

                        Long startThreadTime = System.currentTimeMillis();
                        long produceThreadTime = System.currentTimeMillis();

                        ProducerRecord<String, String> data = new ProducerRecord<String, String>(
                            "test-topic", String.valueOf(keyNo), "{}");
                    producer.send(data);

                        long elapsedTime = produceThreadTime - startThreadTime;
                        ProducerGroup.totalTimeProducing += elapsedTime;
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        producer.flush();
                    }
                }
            });
        }

    }

    public void shutDown() {
        if (executor != null)
            executor.shutdown();

        try {
            if (!executor.awaitTermination(DEFAULT_SLEEP_TIME,
                    TimeUnit.MILLISECONDS)) {
                System.out.println(
                        "Timed out on creating prodcuers threads to shut down,"
                                + " exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
    }

    StringBuffer generate10kCharacters(String s) {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 200; i++) {
            buffer.append(s);
        }
        return buffer;
    }

    String retrieveData(String topicName, String filepath) {

        String toReturn = null;
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = null;
        try {
            Object obj = parser.parse(new FileReader(filePath));
            jsonObject = (JSONObject) obj;
        } catch (Exception e) {
            e.printStackTrace();
        }

        Set<String> groupKeys = jsonObject.keySet();
        for (String key : groupKeys) {
            JSONObject consumerGroupDetails = (JSONObject) jsonObject.get(key);
            for (int i = 0; i < topicList.length; i++) {
                if (topicName != null && topicName.equals(topicList[i])) {
                    toReturn = generate10kCharacters((String) consumerGroupDetails.get(
                            "Top" + i)).toString();
                }
            }
        }

        return toReturn;
    }

    public static void main(String[] args) {
        String topics = args[0]; // List of topics to create seperated by Comma
        String[] topicList = topics.split(",");
        int noOfthreads = DEFAULT_NO_THREADS * topicList.length; // Number of Publishers
        int noOfMessages = Integer.valueOf(args[1]);

        ProducerGroup pg = new ProducerGroup(noOfthreads, noOfMessages, topicList);
        Long startTime = System.currentTimeMillis();
        // Spawning threads
        // bật thread
        pg.run(noOfthreads);

        // từng thread cứ mỗi 5 giây lại kiểm tra lại số message đã gửi đi
        // for (int i = 0; i < topicList.length; i++) {
        //     long noProcessed = 0;
        //     while (noProcessed < noOfMessages) {
        //         synchronized (lock) {
        //             noProcessed = ProducerThread.getTopicCount(topicList[i]);
        //         }
        //         try {
        //             Thread.sleep(DEFAULT_SLEEP_TIME);
        //         } catch (InterruptedException ie) {
        //             ie.printStackTrace();
        //         }
        //     }
        // }


        pg.shutDown();
        Long endTime = System.currentTimeMillis();
        double totalTime = (endTime - startTime);
        double totalTimeInSecs = ((double) totalTime) / 1000;

        long noOfMessagesSent = noOfthreads * noOfMessages;
        System.out.println(" No of Messages produced per second ::" +
                ((double) noOfMessagesSent) / totalTimeInSecs);

    }

}
