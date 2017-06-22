/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.spyglass.solution;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka09.ConsumerStrategies;
import org.apache.spark.streaming.kafka09.KafkaUtils;
import org.apache.spark.streaming.kafka09.LocationStrategies;

import scala.Tuple2;

import com.mapr.spyglass.dao.MetricsDao;
import com.mapr.spyglass.model.Metric;


public class PrintMessagesConsumer {

  public static void main(String[] args) throws Exception {
    final Logger log = Logger.getLogger(PrintMessagesConsumer.class);
    SparkConf sparkConf = new SparkConf().setAppName("PrintMessagesConsumer");
    List<String> topicsList = new ArrayList<>();
    int batchDuration = 30;
    final int alarmDuration,alarmThreshold;
    final String streamName,topicsListString;
    final HashMap<String,String> tagsMap = new HashMap<String,String>();
    
    // Usage: application jar <stream name> <topic1,topic2,topic3> batchDuration
    //topicName = "mfs81.qa.lab_cpu.percent, mfs82.qa.lab_cpu.percent";
    //streamName = "/var/mapr/mapr.monitoring/1854984825002990623";
    if (args != null && args.length==3) {
      try {
      streamName=args[0];
      topicsListString=args[1];
      batchDuration=Integer.parseInt(args[2]);
      } catch (Exception e){
        System.out.println("Failed to start spark job with exception: "+e.getCause());
        printUsage();
        return;
      }
    } else {
      printUsage();
      return;
    }
    List<String> topics = Arrays.asList(topicsListString.split("\\s*,\\s*"));
    // Add the topic to topic set
    for (String topic:topics)
    	topicsList.add(streamName+":"+topic);
    
    
    // Create a streaming context for batch duration.
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchDuration));
    Map<String, Object> kafkaParams = new HashMap<>();
    // cause consumers to start at beginning of topic on first read
    kafkaParams.put("auto.offset.reset", "latest");
    kafkaParams.put("group.id", "spark_consumer");
    kafkaParams.put("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    //  which class to use to deserialize the value of each message
    kafkaParams.put("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");

    // Create direct kafka stream with topics
    JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
        jssc,
        LocationStrategies.PreferConsistent(),
        ConsumerStrategies.<String, String>Subscribe(topicsList, kafkaParams)
    );

    // Get the lines, split them into words and convert it into Metric objects
    JavaDStream<Metric> lines = messages.map(new Function<ConsumerRecord<String, String>, Metric>() {
      private static final long serialVersionUID = -4060881513029929674L;

      @Override
      public Metric call(ConsumerRecord<String, String> tuple2) throws Exception {
      	System.out.println("Consumer Record: "+tuple2);
        Metric metric = Metric.getMetricFromString(tuple2.value().split(" "));
        return metric;
      }
    });
    
    lines.print(100);
    // Start the computation
    jssc.start();
    jssc.awaitTermination();
  }

  public static void printUsage() {
    System.out.println("========================================================");
    System.out.println("Usage: application jar <stream name> <topic1,topic2...> batchDuration");
    System.out.println("========================================================");
  }
}
