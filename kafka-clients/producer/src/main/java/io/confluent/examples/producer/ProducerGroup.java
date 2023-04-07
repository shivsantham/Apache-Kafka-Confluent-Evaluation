package io.confluent.examples.producer;


import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class ProducerGroup {

    private int numThreads;
    private int noOfContinousMessages;
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
        numThreads = noOfThreads;
        noOfContinousMessages = noOfMessages;
        this.topicList = topicList;
        ArrayList<String> topics = new ArrayList<String>(Arrays.asList(topicList));
        ZookeeperUtil.createTopics(topics, noOfPartition, replicationFactor);

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
        int index;
        int count = 0;
        int keyNo = 0;

        // tạo 5 thread
        // mỗi thread đại diện 1 topic
        for (int i = 0; i < numThreads; i++) {
            index = i % DEFAULT_NO_THREADS;
            keyNo = i % 5; // 5 can be replaced with number of topics.
            if (index == 0) {
                count++;
            }
            executor.submit(new ProducerThread(topicList[count - 1], String.valueOf(keyNo),
                    retrieveData(topicList[count - 1],
                            filePath),
                    noOfContinousMessages, producerConfig()));
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
        // int a[] = {1, 2, 3};
        // System.out.println(a.getClass().getName().toString());
        System.out.println("args[0]");
        System.out.println(args.length);
        String topics = args[0]; // List of topics to create seperated by Comma
        String[] topicList = topics.split(",");
        int noOfthreads = DEFAULT_NO_THREADS * topicList.length; // Number of Publishers
        int noOfMessages = Integer.valueOf(args[1]);

        ProducerGroup pg = new ProducerGroup(noOfthreads, noOfMessages, topicList);
        Long startTime = System.currentTimeMillis();
        // Spawning threads
        pg.run(noOfthreads);

        for (int i = 0; i < topicList.length; i++) {
            long noProcessed = 0;
            while (noProcessed < noOfMessages) {
                synchronized (lock) {
                    noProcessed = ProducerThread.getTopicCount(topicList[i]);
                }
                try {
                    Thread.sleep(DEFAULT_SLEEP_TIME);
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
            }
        }

        pg.shutDown();
        Long endTime = System.currentTimeMillis();
        double totalTime = (endTime - startTime);
        double totalTimeInSecs = ((double) totalTime) / 1000;

        long noOfMessagesSent = noOfthreads * noOfMessages;
        System.out.println(" No of Messages produced per second ::" +
                ((double) noOfMessagesSent) / totalTimeInSecs);

    }

}


class ProducerThread implements Runnable {

    private String topicName;
    private String key;
    private String datatoSend;
    private Properties properties;
    private Long threadno;
    private int numMessages;
    // số lượng message đã được gửi tới từng mỗi topic
    private static Map<String, Long> topicCount = new HashMap<String, Long>();
    private static Producer<String, String> producer = null;

    static Object lock = new Object();
    java.util.Date date;

    ProducerThread(String topicName, String key, String datatoSend, int numMessages, Properties properties) {
        this.topicName = topicName;
        this.key = key;
        this.datatoSend = datatoSend;
        this.properties = properties;
        this.numMessages = numMessages;
        date = new java.util.Date();

        synchronized (lock) {
            if (topicCount.get(topicName) == null) {
                topicCount.put(topicName, (long) 0);
            }
        }
    }

    @Override
    public void run() {
        producer = new KafkaProducer<String, String>(properties);
        sendRecords(topicName, key, datatoSend, numMessages, producer);
    }

    public static long getTopicCount(String topic) {
        synchronized (lock) {
            return topicCount.get(topic);
        }
    }

    // topicName
    // key
    // data
    // numMessage
    // producer
    public void sendRecords(String topicName, String key, String dataJSONString, int numMessages,
            Producer<String, String> producer) {

        boolean returnStatus = true;
        try {

            Long startThreadTime = System.currentTimeMillis();

            for (int i = 0; i < numMessages; i++) {
                ProducerRecord<String, String> data = new ProducerRecord<String, String>(
                        topicName, key, dataJSONString);
                producer.send(data);
                synchronized (lock) {
                    topicCount.put(topicName, topicCount.get(topicName) + 1);
                }
            }
            long produceThreadTime = System.currentTimeMillis();
            long elapsedTime = produceThreadTime - startThreadTime;
            ProducerGroup.totalTimeProducing += elapsedTime;
        } catch (Exception e) {
            e.printStackTrace();
            returnStatus = false;
        } finally {
            producer.flush();
        }
    }

}
