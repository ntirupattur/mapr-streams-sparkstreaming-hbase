/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.spyglass.solution;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka09.ConsumerStrategies;
import org.apache.spark.streaming.kafka09.KafkaUtils;
import org.apache.spark.streaming.kafka09.LocationStrategies;

import com.mapr.spyglass.dao.MetricsDao;
import com.mapr.spyglass.model.Metric;
import com.mapr.spyglass.model.Observation;
import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;

import scala.Tuple2;


public class TDigestBuilder {

  private static final Logger log = Logger.getLogger(TDigestBuilder.class);
  
  public static void main(String[] args) throws Exception {
    SparkConf sparkConf = new SparkConf().setAppName("TDigestBuilder"+Math.random());
    List<String> topicsList = new ArrayList<String>();
    final int batchDurationInSec;
    final String streamName,topicNamesList,tagNamesList,tableName;
    
    // Usage: application jar <stream name> <topic1,topic2....> <batchDurationInSec> <tableName> <tagK=tagV,tagK1=tagV1...>
    // streamName = "/var/mapr/mapr.monitoring/metricsStream";
    // topicName = "mfs81.qa.lab_cpu.percent";
    // batchDurationInSec = 30;
    // tableName = "/var/mapr/mapr.monitoring/t-digest";
    // 
    if (args != null && args.length==5) {
      try {
      streamName=args[0];
      topicNamesList=args[1];
      batchDurationInSec = Integer.parseInt(args[2]);
      tableName = args[3];
      tagNamesList = args[4];
      } catch (Exception e){
        System.out.println("Failed to start spark job with exception: "+e.getCause());
        printUsage();
        return;
      }
    } else {
      printUsage();
      return;
    }
    
    List<String> topics = Arrays.asList(topicNamesList.split("\\s*,\\s*"));
    // Add the topic to topic set
    for (String topic:topics)
    	topicsList.add(streamName+":"+topic);
    
    
    // Create a streaming context for batch duration.
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchDurationInSec));
    Map<String, Object> kafkaParams = new HashMap<>();
    // TODO - Make these params configurable
    // cause consumers to start at end of topic on first read
    kafkaParams.put("auto.offset.reset", "latest");
    kafkaParams.put("group.id", "tdigest_builder"+Math.random());
    kafkaParams.put("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    //  which class to use to deserialize the value of each message
    kafkaParams.put("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");

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
        log.info("Tuple value: "+tuple2.value().trim());
        Metric metric = Metric.getMetricFromString(tuple2.value().trim().split(" "));
        return metric;
      }
    });

    // Create a mapped stream of <metric name, metric>
    JavaPairDStream<Tuple2<String,String>, Metric> metricStream = metricsStream.mapToPair(new PairFunction<Metric, Tuple2<String, String>, Metric>() {
      private static final long serialVersionUID = -4060881513029929674L;
      @Override
      public Tuple2<Tuple2<String,String>, Metric> call(Metric t) throws Exception {
        Tuple2 <Tuple2<String,String>,Metric> tuple2=null;
        if (t.getTags()!=null) {
          Tuple2<String,String> key = new Tuple2<String,String>(t.getMetricName(),t.getTags().toString());
          tuple2 = new Tuple2<Tuple2<String,String>, Metric>(key, t);
        } 
        //log.info("Tuple2: "+tuple2.toString());
        return tuple2;
      }
    });
    
    final MetricsDao metricsDao = new MetricsDao(tableName);
    final Calendar now = Calendar.getInstance();
    metricStream.groupByKey().foreachRDD(new VoidFunction<JavaPairRDD<Tuple2<String,String>,Iterable<Metric>>>() {
      private static final long serialVersionUID = 5290630432755196589L;
      @Override
      public void call(JavaPairRDD<Tuple2<String,String>, Iterable<Metric>> arg0)
          throws Exception {
        List<Tuple2<Tuple2<String, String>, Iterable<Metric>>> keyedMetricsList =  arg0.collect();
        if (keyedMetricsList != null && !keyedMetricsList.isEmpty()) {
        	final TDigest td = new MergingDigest(200);
          for (Tuple2<Tuple2<String, String>, Iterable<Metric>> metricsList: keyedMetricsList) {
            int count = 0;
            for (Metric m: metricsList._2()) {
              td.add(m.getValue());
              count++;
            }
            log.info("Adding t-digest for metric: "+metricsList._1()._1()+" with tags"+metricsList._1()._2()+" count "+count);
            Observation o = new Observation(metricsList._1()._1(),metricsList._1()._2(), (Object)td, StringsUtil.getHashForTags(metricsList._1()._2()), batchDurationInSec, count, System.currentTimeMillis(),now.get(Calendar.HOUR_OF_DAY), now.get(Calendar.MINUTE),Observation.type.tdigest);
            metricsDao.addTDigest(o);
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
    System.out.println("Usage: application jar <stream name> <topic name> <batchDuration> <tableName> <tagk1=tagv1,tagk2=tagv2..>");
    System.out.println("========================================================");
  }
}
