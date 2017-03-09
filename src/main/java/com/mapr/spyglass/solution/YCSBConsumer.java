/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.spyglass.solution;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka09.ConsumerStrategies;
import org.apache.spark.streaming.kafka09.KafkaUtils;
import org.apache.spark.streaming.kafka09.LocationStrategies;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import com.tdunning.math.stats.FloatHistogram;

import scala.Tuple2;

import com.mapr.spyglass.dao.MetricsDao;
import com.mapr.spyglass.model.Metric;


public class YCSBConsumer {

  private static final Logger log = Logger.getLogger(YCSBConsumer.class);
  
  public static void main(String[] args) throws Exception {
    SparkConf sparkConf = new SparkConf().setAppName("YCSBConsumer"+Math.random());
    List<String> topicsList = new ArrayList<String>();
    final int batchDuration;
    final String streamName,topicName,tagKey,tableName;
    
    // Usage: application jar <stream name> <topic name> <batchDuration> <tagkey> <tableName>
    // topicName = "mapr.db.latency";
    // streamName = "/var/mapr/mapr.monitoring/YCSB";
    // operationKey = "op";
    // tableName = "/var/mapr/mapr.monitoring/histograms";
    if (args != null && args.length==5) {
      try {
      streamName=args[0];
      topicName=args[1];
      batchDuration = Integer.parseInt(args[2]);
      tagKey = args[3];
      tableName = args[4];
      } catch (Exception e){
        System.out.println("Failed to start spark job with exception: "+e.getCause());
        printUsage();
        return;
      }
    } else {
      printUsage();
      return;
    }
    
    topicsList.add(streamName+":"+topicName);
    
    
    // Create a streaming context for batch duration.
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchDuration));
    Map<String, Object> kafkaParams = new HashMap<>();
    // cause consumers to start at beginning of topic on first read
    kafkaParams.put("auto.offset.reset", "latest");
    kafkaParams.put("group.id", "ycsb_consumer"+Math.random());
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
    JavaInputDStream<ConsumerRecord<String, String>> inputStream = KafkaUtils.createDirectStream(
        jssc,
        LocationStrategies.PreferConsistent(),
        ConsumerStrategies.<String, String>Subscribe(topicsList, kafkaParams)
    );

    // Get the lines, split them into words and convert it into Metric objects
    JavaDStream<Metric> metricsStream = inputStream.map(new Function<ConsumerRecord<String, String>, Metric>() {
      private static final long serialVersionUID = -4060881513029929674L;

      @Override
      public Metric call(ConsumerRecord<String, String> tuple2) throws Exception {
        Metric metric = Metric.getMetricFromString(tuple2.value().split(" "));
        return metric;
      }
    });

    // Create a mapped stream of <metric name, metric>
    JavaPairDStream<Tuple2<String,String>, Metric> metricStream = metricsStream.mapToPair(new PairFunction<Metric, Tuple2<String, String>, Metric>() {
      private static final long serialVersionUID = -4060881513029929674L;
      @Override
      public Tuple2<Tuple2<String,String>, Metric> call(Metric t) throws Exception {
        Tuple2 <Tuple2<String,String>,Metric> tuple2=null;
        String value="";
        if (t.getTags()!=null) {
          value = t.getTags().get(tagKey);
        }
        if (value != null && !value.isEmpty()) {
          Tuple2<String,String> key = new Tuple2<String,String>(value,t.getTags().toString());
          tuple2 = new Tuple2<Tuple2<String,String>, Metric>(key, t);
        } 
        //log.info("Tuple2: "+tuple2.toString());
        return tuple2;
      }
    });
    
    final MetricsDao metricsDao = new MetricsDao(tableName);
    metricStream.groupByKey().foreachRDD(new VoidFunction<JavaPairRDD<Tuple2<String,String>,Iterable<Metric>>>() {
      private static final long serialVersionUID = 5290630432755196589L;
      @Override
      public void call(JavaPairRDD<Tuple2<String,String>, Iterable<Metric>> arg0)
          throws Exception {
        List<Tuple2<Tuple2<String, String>, Iterable<Metric>>> keyedMetricsList =  arg0.collect();
        if (keyedMetricsList != null && !keyedMetricsList.isEmpty()) {
          final FloatHistogram histogram = new FloatHistogram(1, 10000);
          //System.out.println("Size: "+keyedMetricsList.size());
          for (Tuple2<Tuple2<String, String>, Iterable<Metric>> metricsList: keyedMetricsList) {
            double count = 0;
            for (Metric m: metricsList._2()) {
              histogram.add(m.getValue());
              count++;
            }
            System.out.println("Aggregated Tuple2: "+metricsList._1()+" "+count);
            metricsDao.addHistogram(metricsList._1()._1(),System.currentTimeMillis(),metricsList._1()._2(), histogram, topicName, count,batchDuration);
          }
        }
      }
    });
    
    // Start the computation
    jssc.start();
    jssc.awaitTermination();
  }

  public static void printUsage() {
    System.out.println("========================================================");
    System.out.println("Usage: application jar <stream name> <topic name> <tagk1=tagv1,tagk2=tagv2..> <batchDuration> <tableName>");
    System.out.println("========================================================");
  }
}
