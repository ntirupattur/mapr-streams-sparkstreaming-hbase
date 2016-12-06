/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */
package solution;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;


public class StreamsConsumer {

  public static void main(String[] args) throws IOException {

    SparkConf sparkConf = new SparkConf().setAppName("StreamsConsumer");
    // Create the context with 2 seconds batch size
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

    int numThreads = 5;
    Map<String, Integer> topicMap = new HashMap<>();
    String topic = "/var/mapr/mapr.monitoring/1854984825002990623:mfs81.qa.lab_cpu.percent";
    topicMap.put(topic, numThreads);


    JavaPairReceiverInputDStream<String, String> messages =
        KafkaUtils.createStream(jssc, "mfs82.qa.lab", "streamprocessor", topicMap);

    messages.print();
//    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
//      @Override
//      public String call(Tuple2<String, String> tuple2) {
//        return tuple2._2();
//      }
//    });

//    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//      @Override
//      public Iterator<String> call(String x) {
//        return Arrays.asList(SPACE.split(x)).iterator();
//      }
//    });
//
//    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
//        new PairFunction<String, String, Integer>() {
//          @Override
//          public Tuple2<String, Integer> call(String s) {
//            return new Tuple2<>(s, 1);
//          }
//        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//          @Override
//          public Integer call(Integer i1, Integer i2) {
//            return i1 + i2;
//          }
//        });

//    wordCounts.print();
    jssc.start();
    jssc.awaitTermination();
  }


}
