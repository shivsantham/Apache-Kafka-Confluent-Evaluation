package io.confluent.examples.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.serializer.StringDecoder; 
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.utils.VerifiableProperties;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;

public class ConsumerGroup {
	
	
	  private final ConsumerConnector consumer;
	  private final String topic;
	  private ExecutorService executor;
	  private String zookeeper;
	  private String bootStrapServer;
	  private String groupId;
	  private String url;
	  public static long totalTimeConsuming = 0 ; 
	  public static long totalMessagesConsumed = 0;
	  public static boolean workDone = false;
	  int consumeGroup;
	    public static int barriercount = 0;
	    public static double totalTime;
	    static Object lock = new Object();
	    
	    public static long startTime;
	    public static long endTime;

	  public ConsumerGroup(String zookeeper, String groupId, String topic, int consumeGroup) {
	    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
	        new ConsumerConfig(createConsumerConfig(zookeeper, groupId)));
	    this.topic = topic;
	    this.zookeeper = zookeeper;
	    //this.bootStrapServer = bootStrapServer;
	    this.groupId = groupId;
	    this.consumeGroup = consumeGroup;
	    //this.url = url;
	  }

	  private Properties createConsumerConfig(String zookeeper, String groupId) {
	    Properties props = new Properties();
	    props.put("zookeeper.connect", zookeeper);
	    // props.put("bootstrap.servers", bootStrapServer);

	    props.put("consumer.timeout.ms", "1000");
	    props.put("group.id", groupId);
	    //props.put("auto.commit.enable", "true");
	    props.put("auto.commit.interval.ms", "1000");
	    props.put("auto.offset.reset", "smallest");
	    //props.put("schema.registry.url", url);

	    return props;
	  }
	  
	  
	  public void run(int numThreads,CyclicBarrier cb) {
            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
            topicCountMap.put(topic, numThreads);

            Properties props = createConsumerConfig(zookeeper, groupId);
            VerifiableProperties vProps = new VerifiableProperties(props);
            StringDecoder keyDecoder = new StringDecoder(vProps);
            StringDecoder valueDecoder = new StringDecoder(vProps);

            Map<String, List<KafkaStream<String, String>>> consumerMap =
               consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
            List<KafkaStream<String, String>> streams = consumerMap.get(topic);
            
            long totalNoOfMessagesPerTopic = streams.size();
            // Launch all the threads
            executor = Executors.newFixedThreadPool(numThreads);

            // Create ConsumerLogic objects and bind them to threads
            int threadNumber = 0;
            for (final KafkaStream stream : streams) {
                 executor.submit(new ConsumerLogic(stream, threadNumber,consumeGroup,cb));
                 threadNumber++;
            }
            


      }

	  public void pauseForNow(String[] topicList){
	      
		  if(consumer != null) executor.shutdown();
		  if (executor != null) executor.shutdown();
	       try {
	              if (!executor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
	                  System.out.println(
	                     "Timed out waiting on consumer threads to shut down,"
	                  + " exiting uncleanly");
	              }
	       } 
	       catch (InterruptedException e) {
	              System.out.println("Interrupted during shutdown, exiting uncleanly");
	       }
	    }
	  
	  
	/*  public static boolean isAllWorkDone(String[] topicList){
			 
			 //for(Integer i: outerMap.keySet()){
				 
				for(int i=0 ; i<topicList.length;i++){
					if(!ConsumerLogic.isWorkDoneforGroup(i)){
					 //System.out.println("returning false from inner Map");
					 return false;
					}
				}
				
			 //}
		 
			 //System.out.println("OuterMap -> size :" +ConsumerLogic.outerMap.size());
			 return ConsumerLogic.outerMap.isEmpty() ? false : true;
		 }*/
	  
	  
		  public static void main(String[] args) {

          String zooKeeper = "localhost:2181";
          String groupId = "group";
          String topics = args[0];
          String[] topicList = topics.split(",");
          String bootStrapServer = "http://localhost:9092";

          // No of threads per topic 
          int noOfthreads = 10;
          
          //ConsumerGroup[] cg = null;
          List<ConsumerGroup> cg = new ArrayList<ConsumerGroup>();
          ConsumerGroup cga = null;
          

          
          // Launching 5 consumer groups for 5 topics -- with 10 threads in a group 
          
          /*for(int i=0; i<topicList.length;i++){
              ConsumerGroup cg = new ConsumerGroup(zooKeeper, groupId, topicList[i]);
              cg.run(noOfthreads);
          }*/
          startTime = System.currentTimeMillis();
          
          CyclicBarrier cb = new CyclicBarrier(30, new Runnable(){
              @Override
              public void run(){
                  //This task will be executed once 
                 //all thread reaches barrier
                  
                  if (barriercount == Integer.MAX_VALUE) {
                      return;
                  }
                  barriercount++;

        		//  if(barriercount == 1){
                     endTime = System.currentTimeMillis();
        		 // }
                 totalTime = endTime - startTime;
                 double totalTimeInSecs = ((double) totalTime)/1000;
                  
                  if(totalMessagesConsumed > 0) {
                  System.out.println("Total Number of Messages" + totalMessagesConsumed + "Total Time spent" + totalTime);
                  System.out.println( " Consumption Throughput :: " + ((double)totalMessagesConsumed)/totalTimeInSecs + 
                         "Messages per second" );
        	      }
                  
                  synchronized (lock) {
                      totalMessagesConsumed = 0;
                  }                  
                  
        		  //System.out.println("All parties are arrived at barrier, lets play");
              }
          });
           
           
           
           for(int i=0; i<topicList.length;i++){
               cga = new ConsumerGroup(zooKeeper, groupId, topicList[i],i);
               cg.add (cga);
               cga.run(noOfthreads,cb);
           }
          


      }

	  
	  

	  

}
