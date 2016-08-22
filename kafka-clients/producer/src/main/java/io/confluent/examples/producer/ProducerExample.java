/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.producer;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer; 

import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.io.FileReader;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class ProducerExample {
  
	private static Producer<String, String> producer = null;
	private ExecutorService executor;

  public static String BootStrapServer = "http://localhost:9092";

    static {
      Properties props = new Properties();
      props.put("bootstrap.servers", BootStrapServer);
      props.put("acks", "all");
      props.put("retries", 0);
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//      props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
//      props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");

      producer = new KafkaProducer<String,String>(props);



    }


    public static void main(String[] args) {

      boolean flag = sendBulk("sample","key1");
      System.out.println("sendBulk status = "+Boolean.toString(flag));
    }


    public static boolean sendBulk (String topicName, String key)
  {
    boolean returnStatus = true;
    String dataJSONString = null;
    try {


      JSONParser parse = new JSONParser();
      
      try{
      
          Object obj = parse.parse(new FileReader("/ariba/sample.json"));
          //JSONObject jsonObject = (JSONObject)obj;
          JSONArray arr = new JSONArray();
          arr.add(obj);
          
          Iterator it = arr.iterator();
      
          String tmp = null;
          while (it.hasNext()) {
                 Object elem = it.next();
                 //tmp = Object.get("publicKey");
                 
                 if (dataJSONString == null) {
                     dataJSONString = elem.toString();
                 }
                 else {
                     dataJSONString = dataJSONString + "\n" + elem.toString();
                 }
          }
      
      //String dataJSONString = (String)jsonObject.get("publicKey");
      
      
      }
      catch (Exception e){
             e.printStackTrace();
      }
     
      System.out.println("RECORDS STRING ::"+dataJSONString.toString());
      try {
        

//        for (GenericRecord rec : records) {
          ProducerRecord<String, String> data = new ProducerRecord<String, String>(
              topicName, key, dataJSONString.toString());
          System.out.println("before send");
          producer.send(data);
          System.out.println("after send");
          //producerRecords.add(data);
        
        //producerBulk.send(producerRecords);
      } 
      
      catch (Exception e) {
        e.printStackTrace();
        returnStatus = false;
      }
      finally {
        producer.close();
        //producerBulk.flush();
      }
    }
    catch(Exception e) {
      returnStatus = false;
      e.printStackTrace();
    }
    
    return returnStatus;
  }




  
}
