package com.mapr.spyglass.solution;

import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.ojai.Document;

import com.mapr.db.rowcol.DBList;
import com.mapr.spyglass.dao.MetricsDao;
import com.mapr.spyglass.model.Observation;

public class AnomalyDetectorService {

	ExecutorService anomalyDetector;
	private KafkaProducer<String, String> producer;

	public AnomalyDetectorService() {
		anomalyDetector = Executors.newCachedThreadPool();
		configureProducer();
	}

	public void shutdown() {
		try {
			anomalyDetector.shutdown();
			anomalyDetector.awaitTermination(60, TimeUnit.SECONDS);
			producer.close();
		} catch (InterruptedException e) {
			log.error("Exception occurred: "+e.getMessage());
		}
	}
	private static final Logger log = Logger.getLogger(AnomalyDetectorService.class);

	public Callable<Double> calculateEntropy(final QueryRequest request, final Observation o, final String start, final String end, final String hour, final String minute) {

		Callable<Double> task = new Callable<Double>() {
			@Override
			public Double call() {
				log.info("Fetching the historic data");

				int[] currentCounts = (int[]) o.getData();
				int numOfDataPoints = o.getNumOfDataPoints();
				// TODO - Add caching logic here to avoid querying DB tables again and again for same data
				List<Document> documents = request.getDocuments(o.getMetricName(), start, end, String.valueOf(o.getWindowDuration()), o.getTags(), hour, minute);
				long numOfHistoricDataPoints=0;
				int[] historicCounts = new int[33]; // TODO - Make this value configurable
				for (Document d: documents) {
					List<Object> tempCounts = d.getList("counts");
					Object[] tempCountsArray = ((DBList) tempCounts.get(0)).toArray();
					for(int i=0;i<historicCounts.length;i++) {
						historicCounts[i] = historicCounts[i] + (int)tempCountsArray[i];
					}
					numOfHistoricDataPoints = numOfHistoricDataPoints + d.getInt("totalcount");
				}

				double[] currentDistribution = new double[currentCounts.length];
				double[] historicDistribution = new double[historicCounts.length];
				double relativeEntropy = 0.0;
				for (int i=0;i<currentCounts.length;i++) {
					currentDistribution[i] = (double)currentCounts[i]/numOfDataPoints;
					if (numOfHistoricDataPoints != 0)
						historicDistribution[i] = (double)historicCounts[i]/numOfHistoricDataPoints;
					else
						historicDistribution[i] = 0.0;
					//log.info("Current Count: "+currentCounts[i]+" Historic Count: "+historicCounts[i]+" Current Size: "+numOfDataPoints+" Historic Size: "+numOfHistoricDataPoints);
					if (historicDistribution[i] == 0.0 || currentDistribution[i] == 0.0)
						relativeEntropy = relativeEntropy + 0.0;
					else
						relativeEntropy = relativeEntropy + (currentDistribution[i] * Math.log(currentDistribution[i]/historicDistribution[i]));
					//log.info("Current Distribution: "+currentDistribution[i]+" historic distribution: "+historicDistribution[i]+" relative entropy: "+relativeEntropy);
				}

				log.info("Current Distribution: "+Arrays.toString(currentDistribution)+" count: "+numOfDataPoints+" Historic Distribution: "+Arrays.toString(historicDistribution)+" count: "+numOfHistoricDataPoints);
				log.info("Relative entropy for the metric: "+o.getMetricName()+" with tags: "+o.getTags()+" is: "+relativeEntropy+" from "+start+" to "+end+" at "+hour);
				return relativeEntropy;
			}
		};
		return task;
	}

	public void submit(final Observation o, final QueryRequest request, final MetricsDao metricsDao, final int hour, final int minute, final TimeZone timeZone, final String streamName, final double threshold) {
		Runnable task = new Runnable() {
			@Override
			public void run() {
				detectAnomaly(o, request, metricsDao, hour, minute, timeZone, streamName, threshold);
			}
		};
		anomalyDetector.submit(task);
	}

	public void detectAnomaly(final Observation o, final QueryRequest request, final MetricsDao metricsDao, final int hour, final int minute, final TimeZone timeZone, final String streamName, final double threshold) {
		try {
			final String timeZoneId = timeZone.getID();
			// TODO - Add a way to set the trend criteria (like last week, last 1 day etc) via configuration instead of code change
			Future<Double> lastOneHourResult = anomalyDetector.submit(calculateEntropy(request, o, "1h-ago", "now", null, null));

			// Get timestamp of current base hour yesterday
			long oneDayAgo = DateTime.parseDateTimeString("1d-ago", timeZoneId);
			long oneDayAgoBaseHour = DateTime.previousInterval(oneDayAgo, 1, Calendar.HOUR_OF_DAY, timeZone).getTimeInMillis();
			// Get timestamp of base hour for current timestamp
			long currentBaseHour = DateTime.previousInterval(System.currentTimeMillis(), 1, Calendar.HOUR_OF_DAY, timeZone).getTimeInMillis();
			// Calculate the relative entropy for distribution at same base hour yesterday
			Future<Double> yesterdayResult = anomalyDetector.submit(calculateEntropy(request, o, String.valueOf(oneDayAgoBaseHour), String.valueOf(currentBaseHour), String.valueOf(o.getHour()), null));

			// Get timestamp of current base hour same day last week
			long oneWeekAgo = DateTime.parseDateTimeString("7d-ago", timeZoneId);
			long oneWeekAgoBaseHour = DateTime.previousInterval(oneWeekAgo, 1, Calendar.HOUR_OF_DAY, timeZone).getTimeInMillis();
			// Get timestamp of current base hour same day 6d ago
			long sixdaysAgo = DateTime.parseDateTimeString("6d-ago", timeZoneId);
			long sixdaysAgoBaseHour = DateTime.previousInterval(sixdaysAgo, 1, Calendar.HOUR_OF_DAY, timeZone).getTimeInMillis();
			// Calculate the relative entropy for distribution at same base hour same day last week
			Future<Double> lastWeekResult = anomalyDetector.submit(calculateEntropy(request, o, String.valueOf(oneWeekAgoBaseHour), String.valueOf(sixdaysAgoBaseHour), String.valueOf(o.getHour()), null));

			double relativeEntropy = lastOneHourResult.get();
			log.info("Relative Entropy for last 1h:  "+relativeEntropy+" calculated at: "+hour+":"+minute);

			double relativeEntropyYesterday = yesterdayResult.get();
			log.info("Relative Entropy for yesterday:  "+relativeEntropyYesterday+" calculated at: "+hour+":"+minute);


			double relativeEntropyLastWeek = lastWeekResult.get();
			log.info("Relative Entropy for last week:  "+relativeEntropyLastWeek+" calculated at: "+hour+":"+minute);

			//TODO - Make this configurable
			int totalCount = 3;
			int runningCount = 0;
			if ((2*relativeEntropy*o.getNumOfDataPoints()) > threshold) {
				runningCount++;
			}
			if ((2*relativeEntropyYesterday*o.getNumOfDataPoints()) > threshold) {
				runningCount++;
			}
			if ((2*relativeEntropyLastWeek*o.getNumOfDataPoints()) > threshold) {
				runningCount++;
			}

			log.info("Running Count: "+runningCount);

			// TODO - Make this check configurable
			if ( runningCount == totalCount || (double)runningCount/totalCount > 0.6) {
				//put cpu.percent 1500323100 84.8637739656912 fqdn=mfs81.qa.lab  cpu_core=7 cpu_class=idle  clusterid=2992001618649411846 clustername=my.cluster.com
				String value = "put mapr.anomalies "+o.getTimeStamp()+" "+relativeEntropy+" metric="+o.getMetricName()+" "+o.getTags().trim().replaceAll("\\{|\\}", "").replaceAll(",", " ");
				log.info("Sending record "+value);
				ProducerRecord<String, String> rec = new ProducerRecord<String, String>(streamName+":mapr.anomalies",o.getMetricName(), value);
				producer.send(rec);
				log.info("Found anomaly in this distribution with relative entropy: "+relativeEntropy);
			}
			metricsDao.addCounts(o);
		} catch (Exception e) {
			log.error("Failed with exception: "+e.getMessage());
		}

	}

	/* Set the value for a configuration parameter.
  This configuration parameter specifies which class
  to use to serialize the value of each message.*/
	public void configureProducer() {
		Properties props = new Properties();
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<String, String>(props);
	}
}
