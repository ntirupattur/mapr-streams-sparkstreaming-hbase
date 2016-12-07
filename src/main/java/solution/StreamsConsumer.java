/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */
package solution;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.v09.KafkaUtils;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;

import scala.Tuple2;


public class StreamsConsumer {

  public static void main(String[] args) throws Exception {
    SparkConf sparkConf = new SparkConf().setAppName("StreamsConsumer");
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
    
    String topicName = "/var/mapr/mapr.monitoring/1854984825002990623:mfs81.qa.lab_cpu.percent";
    Set<String> topicsSet = new HashSet<>();
    topicsSet.add(topicName);
    
    Map<String, String> kafkaParams = new HashMap<>();
    Properties props = new Properties();
    // cause consumers to start at beginning of topic on first read
    kafkaParams.put("auto.offset.reset", "earliest");
    kafkaParams.put("group.id", "spark_consumer");
    kafkaParams.put("key.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");
    //  which class to use to deserialize the value of each message
    kafkaParams.put("value.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");
    
    // Create direct kafka stream with topics
    JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
        jssc,
        String.class,
        String.class,
        kafkaParams,
        topicsSet
    );

    // Get the lines, split them into words, count the words and print
    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
      @Override
      public String call(Tuple2<String, String> tuple2) {
        return tuple2._2();
      }
    });
    
    lines.print();
//    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//      @Override
//      public Iterator<String> call(String x) {
//        return Arrays.asList(SPACE.split(x)).iterator();
//      }
//    });
//    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
//      new PairFunction<String, String, Integer>() {
//        @Override
//        public Tuple2<String, Integer> call(String s) {
//          return new Tuple2<>(s, 1);
//        }
//      }).reduceByKey(
//        new Function2<Integer, Integer, Integer>() {
//        @Override
//        public Integer call(Integer i1, Integer i2) {
//          return i1 + i2;
//        }
//      });
//    wordCounts.print();

    // Start the computation
    jssc.start();
    jssc.awaitTermination();
  }


}
