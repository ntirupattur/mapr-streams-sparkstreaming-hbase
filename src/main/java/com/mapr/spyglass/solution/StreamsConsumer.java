/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.spyglass.solution;


import java.util.ArrayList;
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


public class StreamsConsumer {

  public static void main(String[] args) throws Exception {
    final Logger log = Logger.getLogger(StreamsConsumer.class);
    SparkConf sparkConf = new SparkConf().setAppName("StreamsConsumer");
    List<String> topicsList = new ArrayList<>();
    int batchDuration = 0;
    final int alarmDuration,alarmThreshold;
    final String streamName,topicName,metricTags,hostName,tableName;
    final HashMap<String,String> tagsMap = new HashMap<String,String>();
    
    // Usage: application jar <stream name> <topic name> <tagk1=tagv1,tagk2=tagv2..> <batchDuration> <alertDuration> <alertThreshold> <hostname> <tableName>
    //topicName = "/var/mapr/mapr.monitoring/1854984825002990623:mfs81.qa.lab_cpu.percent";
    //streamName = "/var/mapr/mapr.monitoring/1854984825002990623";
    // tableName = "/var/mapr/mapr.monitoring/aggregates";
    if (args != null && args.length==8) {
      try {
      streamName=args[0];
      topicName=args[1];
      metricTags = args[2]; 
      batchDuration = Integer.parseInt(args[3]);
      alarmDuration = Integer.parseInt(args[4]);
      alarmThreshold = Integer.parseInt(args[5]);
      hostName = args[6];
      tableName = args[7];
      } catch (Exception e){
        System.out.println("Failed to start spark job with exception: "+e.getCause());
        printUsage();
        return;
      }
    } else {
      printUsage();
      return;
    }
    
    // Add the topic to topic set
    topicsList.add(streamName+":"+topicName);
    
    // TODO - Raise alarm based on the tags criteria - Check and Add alarms dynamically
    final String[] raiseAlarmCommand = new String[] {"maprcli", "alarm", "raise", "-alarm", "NODE_ALARM_CHECK_CPU_IDLE", "-description", "Low CPU Idle %", "-entity", hostName};
    
    // Parse the tags string to key,value pairs
    String[] tagPairs = Metric.splitString(metricTags, ',');
    for (int i=0;i<tagPairs.length;i++) {
      Metric.parse(tagsMap, tagPairs[i]);
    }
    
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

    //    final Configuration conf = new Configuration();
    //    final Map<String, List<TopicFeedInfo>> topicsMap;
    //    Admin admin = Streams.newAdmin(conf);
    //    MarlinAdminImpl madmin = (MarlinAdminImpl)admin;
    //    topicsMap = madmin.listTopics(streamName);
    //    Iterator entries = topicsMap.entrySet().iterator();
    //    while (entries.hasNext()) {
    //      Map.Entry entry = (Map.Entry) entries.next();
    //      String topicName = (String) entry.getKey();
    //      topicName = streamName+":"+topicName;
    //      topicsSet.add(topicName);
    //    }

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
        Metric metric = Metric.getMetricFromString(tuple2.value().split(" "));
        return metric;
      }
    });

    
    // Create a mapped stream of <metric name, metric>
    JavaPairDStream<String, Metric> metricStream = lines.mapToPair(new PairFunction<Metric, String, Metric>() {
      private static final long serialVersionUID = -4060881513029929674L;
      @Override
      public Tuple2<String, Metric> call(Metric t) throws Exception {
        Tuple2 <String,Metric> tuple2 = new Tuple2<String, Metric>(t.getMetricName(), t);
        return tuple2;
      }
    });

    
    // Filter the keyed stream based on tags
    JavaPairDStream<String, Metric> metricsByKeyStream = metricStream.filter(new Function<Tuple2<String,Metric>, Boolean>() {

      @Override
      public Boolean call(Tuple2<String, Metric> v1) throws Exception {
        for (Entry<String, String> e: tagsMap.entrySet()) {
          if (v1._2.getTags().containsKey(e.getKey())) { //TODO - Code to handle more than one <tagk=tagv> pair
            if (v1._2.getTags().get(e.getKey()).equals(e.getValue())) 
              return true;
          }
        }
        return false;
      }
    });

   
    final MetricsDao metricsDao = new MetricsDao(tableName);
    
    // Group all the metrics matching the filter criteria for batch duration to create an aggregated stream
    JavaPairDStream<String, Metric> aggregatedStream = metricsByKeyStream.reduceByKeyAndWindow(new Function2<Metric, Metric, Metric>() {
      private static final long serialVersionUID = -4060881513029929674L;
      @Override
      public Metric call(Metric v1, Metric v2) throws Exception {
        Metric newMetric = new Metric();
        if (v1.getTimeStamp() == v2.getTimeStamp()) {
          newMetric.setMetricName(v1.getMetricName());
          newMetric.setTimeStamp(v1.getTimeStamp());
          newMetric.setValue(v1.getValue()+v2.getValue());
          tagsMap.put("value", "aggregated");
          newMetric.setTags(tagsMap);
          return newMetric;
        } else {
          return v1;
        }

      }

    },Durations.seconds(batchDuration));
    
    // Filter the stream for aggregated metrics
    JavaPairDStream<String, Metric> filteredAggregatedStream = aggregatedStream.filter(new Function<Tuple2<String,Metric>, Boolean>() {
      @Override
      public Boolean call(Tuple2<String, Metric> v1) throws Exception {
        if(v1._2().getTags().containsValue("aggregated")) return true;
        return false;
      }
    });
    
    
    // For each such metric, test it against the model and raise alarm if there is an anomaly
    filteredAggregatedStream.foreachRDD(new VoidFunction<JavaPairRDD<String,Metric>>() {
      @Override
      public void call(JavaPairRDD<String, Metric> v1) throws Exception {
        if (v1 != null){
          List<Tuple2<String,Metric>> metrics = v1.collect();
          if (metrics!= null && !metrics.isEmpty()) {
            boolean raiseAlarm = metricsDao.updateAggregates(metrics.get(0)._2(),alarmThreshold,alarmDuration,false);
            if (raiseAlarm) {
              Runtime.getRuntime().exec(raiseAlarmCommand);
            }
          }
        }
      }
    });
    
    
//    filteredAggregatedStream.foreachRDD(new Function<JavaPairRDD<String,Metric>, Void>() {
//      @Override
//      public Void call(JavaPairRDD<String, Metric> v1) throws Exception {
//        if (v1 != null){
//          List<Tuple2<String,Metric>> metrics = v1.collect();
//          if (!metrics.isEmpty()) {
//            boolean raiseAlarm = metricsDao.updateAggregates(metrics.get(0)._2(),alarmThreshold,alarmDuration,false);
//            if (raiseAlarm) {
//              Runtime.getRuntime().exec(raiseAlarmCommand);
//            }
//          }
//        }
//        return null;
//      }
//    });
//    
    // Start the computation
    jssc.start();
    jssc.awaitTermination();
  }

  public static void printUsage() {
    System.out.println("========================================================");
    System.out.println("Usage: application jar <stream name> <topic name> <tagk1=tagv1,tagk2=tagv2..> <batchDuration> <alertDuration> <alertThreshold> <hostname> <tableName>");
    System.out.println("========================================================");
  }
}
