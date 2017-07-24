/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.spyglass.solution;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka09.ConsumerStrategies;
import org.apache.spark.streaming.kafka09.KafkaUtils;
import org.apache.spark.streaming.kafka09.LocationStrategies;

import com.mapr.spyglass.model.Metric;


public class StreamsUnionGenerator {

	private static final Logger log = Logger.getLogger(StreamsUnionGenerator.class);

	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName("StreamsUnionGenerator"+Math.random());
		final int batchDurationInSec;
		final String streamName,topic1, topic2;

		// Usage: application jar <stream name> topic1 topic2 <batchDurationInSec>
		// streamName = "/var/mapr/mapr.monitoring/metricsStream";
		// topic1 = "mapr.nm.containers_launched";
		// topic2 = "mapr.nm.containers_failed";
		// batchDurationInSec = 30;

		if (args != null && args.length==4) {
			try {
				streamName=args[0];
				topic1=args[1];
				topic2=args[2];
				batchDurationInSec = Integer.parseInt(args[3]);
			} catch (Exception e){
				System.out.println("Failed to start spark job with exception: "+e.getCause());
				printUsage();
				return;
			}
		} else {
			printUsage();
			return;
		}

		// Create a streaming context for batch duration.
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchDurationInSec));
		Map<String, Object> kafkaParams = new HashMap<>();
		// TODO - Make these params configurable
		// cause consumers to start at end of topic on first read
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("group.id", "streams_union"+Math.random());
		kafkaParams.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		//  which class to use to deserialize the value of each message
		kafkaParams.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");

		JavaInputDStream<ConsumerRecord<String, String>> stream1 = KafkaUtils.createDirectStream(
				jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>SubscribePattern(Pattern.compile(streamName+":.*"+topic1), kafkaParams)
				);

		JavaInputDStream<ConsumerRecord<String, String>> stream2 = KafkaUtils.createDirectStream(
				jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>SubscribePattern(Pattern.compile(streamName+":.*"+topic2), kafkaParams)
				);

		// Get the lines, split them into words and convert it into Metric objects
		JavaDStream<Metric> metricsStream1 = stream1.map(new Function<ConsumerRecord<String, String>, Metric>() {
			@Override
			public Metric call(ConsumerRecord<String, String> tuple2) throws Exception {
				log.info("Stream1 value: "+tuple2.value().trim());
				Metric metric = Metric.getMetricFromString(tuple2.value().trim().split(" "));
				return metric;
			}
		});


		JavaDStream<Metric> metricsStream2 = stream2.map(new Function<ConsumerRecord<String, String>, Metric>() {
			@Override
			public Metric call(ConsumerRecord<String, String> tuple2) throws Exception {
				log.info("Stream2 value: "+tuple2.value().trim());
				Metric metric = Metric.getMetricFromString(tuple2.value().trim().split(" "));
				return metric;
			}
		});


		JavaDStream<Metric> mergedStream = metricsStream1.union(metricsStream2);
		mergedStream.foreachRDD(new VoidFunction<JavaRDD<Metric>>() {

			@Override
			public void call(JavaRDD<Metric> arg0) throws Exception {
				List<String> results = QueryRequest.getLLR(arg0.collect(),topic1, topic2);
				for(String result: results) {
					log.info("Result: "+result);
				}
			}
		});

		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}

	public static void printUsage() {
		System.out.println("========================================================");
		System.out.println("Usage: application jar <stream name> <topic1> >topic2> <batchDuration>	");
		System.out.println("========================================================");
	}
}
