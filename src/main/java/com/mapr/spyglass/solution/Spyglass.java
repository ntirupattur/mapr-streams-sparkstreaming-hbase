/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.spyglass.solution;


import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.apache.spark.HashPartitioner;
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
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka09.ConsumerStrategies;
import org.apache.spark.streaming.kafka09.KafkaUtils;
import org.apache.spark.streaming.kafka09.LocationStrategies;

import com.mapr.spyglass.dao.MetricsDao;
import com.mapr.spyglass.model.Metric;
import com.mapr.spyglass.model.Observation;

import scala.Tuple2;

public class Spyglass {

	private static final Logger log = Logger.getLogger(Spyglass.class);

	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName("Spyglass"+Math.random());
		final int batchDurationInSec;
		final String streamName,topicName,tagNamesList,tableName;
		// TODO - Make these values configurable
		final double min=0.0, max=100.0;
		final int binCount=33;
		final double threshold = 15.134; // Corresponds to N-1 bins of freedom for 0.99 P 

		// Usage: application jar <stream name> <topic1,topic2....> <batchDurationInSec> <tableName> <tagK=tagV,tagK1=tagV1...>
		// streamName = "/var/mapr/mapr.monitoring/metricsStream";
		// topicName = "mfs81.qa.lab_cpu.percent" OR "cpu.percent" 
		// batchDurationInSec = 30;
		// tableName = "/var/mapr/mapr.monitoring/t-digest";
		// tags = "cpu_core=0" OR "fqdn=mfs81.qa.lab,cpu_core=0"; 

		if (args != null && args.length>=4) {
			try {
				streamName=args[0];
				topicName=args[1];
				batchDurationInSec = Integer.parseInt(args[2]);
				tableName = args[3];
				if (args.length==5) {
					tagNamesList = args[4];
				}
			} catch (Exception e){
				log.error("Failed to start spark job with exception: "+e.getCause());
				printUsage();
				return;
			}
		} else {
			printUsage();
			return;
		}

		// Create a streaming context for batch duration.
		final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchDurationInSec));
		Map<String, Object> kafkaParams = new HashMap<>();
		// TODO - Make these params configurable
		// cause consumers to start at end of topic on first read
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("group.id", "spyglass"+Math.random());
		kafkaParams.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		//  which class to use to deserialize the value of each message
		kafkaParams.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");

		// Create direct kafka stream with topics
		JavaInputDStream<ConsumerRecord<String, String>> inputStream = KafkaUtils.createDirectStream(
				jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>SubscribePattern(Pattern.compile(streamName+":.*"+topicName), kafkaParams)
				);

		// Get the lines, split them into words and convert it into Metric objects
		JavaDStream<Metric> metricsStream = inputStream.map(new Function<ConsumerRecord<String, String>, Metric>() {
			private static final long serialVersionUID = -4060881513029929674L;

			@Override
			public Metric call(ConsumerRecord<String, String> tuple2) throws Exception {
				//log.info("Key: "+tuple2.key().trim());
				//log.info("Tuple value: "+tuple2.value().trim());
				Metric metric = Metric.getMetricFromString(tuple2.value().trim().split(" "));
				return metric;
			}
		});

		//TODO - Evaluate how to filter metrics based on tags
		final AnomalyDetectorService anomalyDetector = new AnomalyDetectorService();
		// for graceful shutdown of the application ...
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				log.info("Shutting down streaming app...");
				anomalyDetector.shutdown();
				jssc.stop(true, true);
				log.info("Shutdown of streaming app complete.");
			}
		});

		final MetricsDao metricsDao = new MetricsDao(tableName);
		final QueryRequest request = new QueryRequest(tableName);

		// Create a mapped stream of <metricname+tags, value>
		JavaPairDStream<Tuple2<String,String>, Double> metricStream = metricsStream.mapToPair(new PairFunction<Metric, Tuple2<String, String>, Double>() {
			private static final long serialVersionUID = -4060881513029929674L;
			@Override
			public Tuple2<Tuple2<String,String>, Double> call(Metric t) throws Exception {
				Tuple2 <Tuple2<String,String>,Double> tuple2=null;
				if (t.getTags()!=null) {
					Tuple2<String,String> key = new Tuple2<String,String>(t.getMetricName(),t.getTags().toString());
					tuple2 = new Tuple2<Tuple2<String,String>, Double>(key, Double.valueOf(t.getValue()));
				} 
				//log.info("Tuple2: "+tuple2.toString());
				return tuple2;
			}
		});

		JavaPairDStream<Tuple2<String,String>, Tuple2<int[], Integer>> aggregatedKeyStream = metricStream.combineByKey(new Function<Double, Tuple2<int[], Integer>>() {
			@Override
			public Tuple2<int[], Integer> call(Double paramT1) throws Exception {
				int[] counts = new int[binCount];
				int discreteValue = discretize(paramT1.doubleValue(),min,max,binCount);
				counts[discreteValue] = counts[discreteValue]+1;
				return new Tuple2<int[], Integer>(counts, 1);
			}
		}, new Function2<Tuple2<int[], Integer>, Double, Tuple2<int[], Integer>>() {
			@Override
			public Tuple2<int[], Integer> call(Tuple2<int[], Integer> paramT1, Double paramT2) throws Exception {
				int discreteValue = discretize(paramT2.doubleValue(),min,max,binCount);
				int[] counts = paramT1._1();
				counts[discreteValue] = counts[discreteValue]+1;
				return new Tuple2<int[], Integer>(counts, paramT1._2+1);
			}

		}, new Function2<Tuple2<int[], Integer>, Tuple2<int[], Integer>, Tuple2<int[], Integer>>() {
			@Override
			public Tuple2<int[], Integer> call(Tuple2<int[], Integer> paramT1, Tuple2<int[], Integer> paramT2) throws Exception {
				int[] counts = new int[binCount];
				int[] countArray1 = paramT1._1();
				int[] countArray2 = paramT2._1();
				for(int i=0;i<counts.length;i++) {
					counts[i] = countArray1[i] + countArray2[i];
				}
				return new Tuple2<int[], Integer>(counts, paramT1._2+paramT2._2);
			}
		}, new HashPartitioner(jssc.ssc().conf().getInt("spark.default.parallelism",5)));

		aggregatedKeyStream.foreachRDD(new VoidFunction<JavaPairRDD<Tuple2<String,String>,Tuple2<int[],Integer>>>() {
			@Override
			public void call(JavaPairRDD<Tuple2<String, String>, Tuple2<int[], Integer>> paramT) throws Exception {
				List<Tuple2<Tuple2<String, String>, Tuple2<int[], Integer>>> keyedMetricsList = paramT.collect();
				if (keyedMetricsList != null && !keyedMetricsList.isEmpty()) {
					for (Tuple2<Tuple2<String, String>, Tuple2<int[], Integer>> aggregatedMetric: keyedMetricsList) {
						log.info("Adding count for metric: "+aggregatedMetric._1()._1()+" with tags"+aggregatedMetric._1()._2()+" total count "+Arrays.toString(aggregatedMetric._2._1())+" "+aggregatedMetric._2._2());
						final Calendar now = Calendar.getInstance();
						final TimeZone timeZone = now.getTimeZone();
						final int hour = now.get(Calendar.HOUR_OF_DAY);
						final int minute = now.get(Calendar.MINUTE);
						final Observation o = new Observation(aggregatedMetric._1()._1(),aggregatedMetric._1()._2(), (Object)aggregatedMetric._2()._1(), StringsUtil.getHashForTags(aggregatedMetric._1()._2()), batchDurationInSec, aggregatedMetric._2._2(), System.currentTimeMillis(),hour,minute,Observation.type.countarray);
						anomalyDetector.submit(o, request, metricsDao, hour, minute, timeZone, streamName, threshold);
					}
				}
			}
		});

		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}

	public static double normalize(double value, double min, double max) {
		return (value - min) / (max - min);
	}

	public static int clamp(int value, int min, int max) {
		if (value < min) value = min;
		if (value > max) value = max;
		return value;
	}

	public static int discretize(double value, double min, double max, int binCount) {
		int discreteValue = (int) (binCount * normalize(value, min, max));
		return clamp(discreteValue, 0, binCount - 1);
	}

	public static void printUsage() {
		System.out.println("========================================================");
		System.out.println("Usage: application jar <stream name> <topic name> <batchDuration> <tableName> <tagk1=tagv1,tagk2=tagv2..>");
		System.out.println("========================================================");
	}
}
