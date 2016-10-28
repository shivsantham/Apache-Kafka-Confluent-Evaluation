package io.confluent.examples.producer;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.io.FileReader;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import io.confluent.examples.producer.ZookeeperUtil;
import io.confluent.examples.producer.ProducerThread;
import java.util.Map;
import java.util.HashMap;

public class ProducerGroup {

    private int numThreads;
    private int noOfContinousMessages;
    private ExecutorService executor;
    private static int DEFAULT_NO_THREADS = 10;
    private static int DEFAULT_SLEEP_TIME = 5000;
    private int noOfPartition = 5;
    private int replicationFactor = 1;	
    public static String BootStrapServer = "http://localhost:9092";
    public static long totalTimeProducing;
    static Object lock = new Object();
    public String filePath = "/ariba/something.json";
    private String[] topicList = null;
    
    ProducerGroup(int noOfThreads, int noOfMessages,String[] topicList) {
      numThreads = noOfThreads;
      noOfContinousMessages = noOfMessages;
      this.topicList = topicList;
      ArrayList<String>topics = new ArrayList<String>(Arrays.asList(topicList));
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
      executor = Executors.newFixedThreadPool(numThreads);
      int index;
      int count = 0;
      int keyNo = 0;
      for (int i=0; i< numThreads; i++) {
           index = i%DEFAULT_NO_THREADS;
           keyNo = i%5; // 5 can be replaced with number of topics.
           if(index==0){count++;}
             executor.submit(new ProducerThread(topicList[count-1],String.valueOf(keyNo),
                             retrieveData(topicList[count-1],
                             filePath),noOfContinousMessages,producerConfig()));
      }
      
    }
    
    public void shutDown(){
       if (executor != null) executor.shutdown();
       
       try {
             if (!executor.awaitTermination(DEFAULT_SLEEP_TIME, 
		          TimeUnit.MILLISECONDS)) {
                  System.out.println(
                     "Timed out on creating prodcuers threads to shut down,"
                  + " exiting uncleanly");
              }
       } 
       catch (InterruptedException e) {
              System.out.println("Interrupted during shutdown, exiting uncleanly");
       }
    }
    
    StringBuffer generate10kCharacters(String s){
        StringBuffer buffer = new StringBuffer();
        for(int i =0 ; i<200;i++){    
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
      } 
      catch (Exception e) {
          e.printStackTrace();
      }

      Set<String> groupKeys = jsonObject.keySet();
      for (String key : groupKeys) {
           JSONObject consumerGroupDetails = (JSONObject) jsonObject.get(key);
                for(int i=0; i<topicList.length;i++){
                    if(topicName != null && topicName.equals(topicList[i])){
                           toReturn = generate10kCharacters((String)consumerGroupDetails.get(
				      "Top"+i)).toString();
                    }
                }
      }

      return toReturn;
    }
    
    public static void main(String[] args){
      
      String topics = args[0]; // List of topics to create seperated by Comma
      String[] topicList = topics.split(",");
      int noOfthreads = DEFAULT_NO_THREADS * topicList.length; // Number of Publishers
      int noOfMessages = Integer.valueOf(args[1]);
      
      ProducerGroup pg = new ProducerGroup(noOfthreads,noOfMessages,topicList);
      Long startTime = System.currentTimeMillis();
      //Spawning threads
      pg.run(noOfthreads);
        
        for (int i=0; i<topicList.length;i++) {
            long noProcessed = 0;
            while (noProcessed < noOfMessages) {
                   synchronized(lock){ 
                     noProcessed = ProducerThread.getTopicCount(topicList[i]);
                  }
		  try {
                     Thread.sleep(DEFAULT_SLEEP_TIME);
                  } 
                  catch (InterruptedException ie) {
                         ie.printStackTrace();
		  }
             }
        }
        
        pg.shutDown();
        Long endTime = System.currentTimeMillis();
        double totalTime = (endTime - startTime);
        double totalTimeInSecs = ((double) totalTime)/1000;
        
        long noOfMessagesSent = noOfthreads * noOfMessages;
        System.out.println( " No of Messages produced per second ::" + 
                   ((double)noOfMessagesSent)/totalTimeInSecs);
        
     }


    
    
	 

}
