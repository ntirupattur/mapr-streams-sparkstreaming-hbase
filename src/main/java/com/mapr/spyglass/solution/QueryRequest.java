/**
 * Class to query the data from JSON-DB tables
 */
package com.mapr.spyglass.solution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.log4j.Logger;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.QueryCondition;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import com.mapr.db.exceptions.TableNotFoundException;
import com.mapr.spyglass.model.Metric;
import com.mapr.spyglass.model.Observation;
import com.tdunning.math.stats.FloatHistogram;


/**
 * @author ntirupattur
 *
 */
public class QueryRequest {
	/*
	 * Usage: com.mapr.spyglass.solution.QueryRequest <tableName> <tagKey> <fromDuration> <toDuration> <windowDurationInSecs> <threshold> <documentType> [<tagk1=tagv1,tagk2=tagv2..>]
	 * Example 1: com.mapr.spyglass.solution.QueryRequest /var/mapr/mapr.monitoring/histograms/ insert 1488322451462 1488913082690 60 1000 histogram [op=insert]
	 * Example 2: com.mapr.spyglass.solution.QueryRequest /var/mapr/mapr.monitoring/t-digest/ cpu.percent 1m-ago now 60 10 tdigest [cpu_core=0, cpu_class=idle]
	 * Example 3: com.mapr.spyglass.solution.QueryRequest /var/mapr/mapr.monitoring/histograms/ insert 1488322451462 1488913082690 60 20 histogram [op=insert,fqdn=qa102-40.qa.lab]
	 */
	private static final Logger log = Logger.getLogger(QueryRequest.class);
	private Table table;
	private LRUConcurrentCache<String, List<Document>> cache;

	public QueryRequest(String tableName) throws Exception{

		if (!(MapRDB.tableExists(tableName))) {
			throw new TableNotFoundException("Table: " + tableName + " does'nt exist");
		}
		this.table = MapRDB.getTable(tableName);
		this.cache = new LRUConcurrentCache<String, List<Document>>(500);
	}

	public static void main(String[] args) {
		if (args.length < 6) {
			printUsage();
			return;
		}

		String tableName = args[0];
		String tagKey = args[1];
		String fromDuration = args[2];
		String toDuration = args[3];
		String windowDurationInSec = args[4];
		String threshold = args[5];
		String documentType = args[6];
		String tags = "";

		if (args.length == 8) {
			tags = args[7];
		}

		QueryRequest request;
		try {
			request = new QueryRequest(tableName);
			//List<String> results = performComputations(tableName, tagKey, fromDuration, toDuration, windowDurationInSec, threshold,tags,documentType);
			List<Document> documentsList = request.getDocuments(tagKey, fromDuration, toDuration, windowDurationInSec, tags, null, null);
			for (Document d : documentsList) {
				System.out.println(d.getId());
				System.out.println(d.getInt("count"));
				System.out.println(d.getString("tags"));
				System.out.println(d.getString("hash"));
				System.out.println(d.getInt("minute"));
				System.out.println(d.getInt("hour"));
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static Map<String, FloatHistogram> getAggregatedHistogram(String tableName, String tagKey,
			String fromDuration, String toDuration, String windowDurationInSec, String tags) throws Exception {
		Map<String, FloatHistogram> histogramMap = new HashMap<String, FloatHistogram>();
		QueryRequest request = new QueryRequest(tableName);
		List<Document> documentsList = request.getDocuments(tagKey, fromDuration, toDuration, windowDurationInSec, tags, null,null);
		for (Document d : documentsList) {
			String key = d.getString("tags").trim().replaceAll("\\{|\\}", "");
			FloatHistogram histogram = (FloatHistogram) SerializationUtils
					.deserialize(Base64.decodeBase64(d.getString("histogram")));
			if (histogramMap.containsKey(key)) {
				FloatHistogram h = (FloatHistogram) histogramMap.get(key);
				histogram.add(h);
			}
			histogramMap.put(key, histogram);
		}
		return histogramMap;
	}

	public static List<String> performComputations(String tableName, String tagKey, String fromDuration, String toDuration, String windowDurationInSec, String threshold, String tags, String documentType) {
		List<String> results = new ArrayList<String>();
		try {
			QueryRequest request = new QueryRequest(tableName);
			List<Document> documentsList = request.getDocuments(tagKey, fromDuration, toDuration, windowDurationInSec, tags, null, null);
			List<Observation> observations = new ArrayList<Observation>();
			int count = 0;
			for (Document d : documentsList) {
				FloatHistogram histogram = (FloatHistogram) SerializationUtils
						.deserialize(Base64.decodeBase64(d.getString(documentType)));
				String metricName = d.getString("metricname");
				int windowDuration = d.getInt("windowduration");
				int operationCount = d.getInt("count");
				count += operationCount;
				observations.add(new Observation(metricName, d.getString("tags"), histogram, d.getString("hash"),windowDuration, operationCount,d.getLong("timestamp"), d.getInt("hour"), d.getInt("minute"),Observation.type.floathistogram));
			}

			results.add("Total Operations Count: " + count);
			getLLRHistogramData(Integer.parseInt(threshold), observations, results);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return results;
	}

	public List<Document> getDocuments(String documentKey, String fromDuration, String toDuration, String windowDurationInSec, String tags, String hourOfDay, String minuteOfHour) {
		List<Document> documentsList = new ArrayList<Document>();
		try {
			//log.info("Table: "+table.getPath());
			//	get Calendar instance
			long startTime = System.currentTimeMillis();
			Calendar now = Calendar.getInstance();
			//get current TimeZone using getTimeZone method of Calendar class
			TimeZone timeZone = now.getTimeZone();
			String timeZoneId = timeZone.getID();
			//log.info("Time Zone: "+ timeZoneId);
			//log.info("Document Key: "+documentKey);
			StringBuffer conditionBuffer = new StringBuffer();

			String fromId = documentKey+DateTime.parseDateTimeString(fromDuration, timeZoneId);
			conditionBuffer.append(fromId);
			String toId = documentKey+(((toDuration == null) || (toDuration.isEmpty())) ? System.currentTimeMillis(): DateTime.parseDateTimeString(toDuration,  timeZoneId));
			conditionBuffer.append(toId);

			QueryCondition condition = MapRDB.newCondition().and().is("_id", QueryCondition.Op.GREATER_OR_EQUAL, fromId)
					.is("_id", QueryCondition.Op.LESS_OR_EQUAL, toId)
					.is("windowduration", QueryCondition.Op.EQUAL, Integer.parseInt(windowDurationInSec));

			if ((tags != null) && (!(tags.isEmpty()))) {
				String tagsHash = StringsUtil.getRegexForTags(tags);
				conditionBuffer.append(tagsHash);
				QueryCondition optionalCondition = MapRDB.newCondition()
						.matches("hash", tagsHash).build();
				condition.condition(optionalCondition);
			}

			if ((hourOfDay != null) && (!(hourOfDay.isEmpty()))) {
				conditionBuffer.append(hourOfDay);
				QueryCondition optionalCondition1 = MapRDB.newCondition()
						.is("hour", QueryCondition.Op.EQUAL, Integer.parseInt(hourOfDay)).build();
				condition.condition(optionalCondition1);
			}

			if ((minuteOfHour != null) && (!(minuteOfHour.isEmpty()))) {
				conditionBuffer.append(minuteOfHour);
				QueryCondition optionalCondition2 = MapRDB.newCondition()
						.is("minute", QueryCondition.Op.EQUAL, Integer.parseInt(minuteOfHour)).build();
				condition.condition(optionalCondition2);
			}

			condition.close().build();

			log.info("Cache size: "+cache.size());

			if (cache.get(conditionBuffer.toString().trim()) != null) {
				documentsList = (List<Document>) cache.get(conditionBuffer.toString().trim());
				log.info("Found in the cache for condition: "+conditionBuffer.toString().trim());
			} else {
				log.info("Fetching from DB for condition: " + conditionBuffer.toString().trim());
				DocumentStream docStream = table.find(condition);
				Iterator<Document> documentsIterator = docStream.iterator();
				while (documentsIterator.hasNext()) {
					Document d = (Document) documentsIterator.next();
					documentsList.add(d);
				}
				cache.put(conditionBuffer.toString().trim(), documentsList);
			}

			log.info("Documents Size: "+documentsList.size());
			log.info("Time taken: "+(System.currentTimeMillis() - startTime));
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Failed with exception: "+e.getMessage());
		}

		return documentsList;
	}

	public static List<String> getLLR(List<Metric> metricsData, String topic1, String topic2) {
		List<String> results = new ArrayList<String>();
		Multiset<String> k11 = HashMultiset.create();
		Multiset<String> k12 = HashMultiset.create();
		int kx1 = 0;
		int kx2 = 0;
		results.add("Documents Size: " + metricsData.size());
		for (Metric metric : metricsData) {
			int toLeft = 0;
			int toRight = 0;
			List<String> tagsList = Arrays.asList(metric.getTags().toString().trim().replaceAll("\\{|\\}", "").split("\\s*,\\s*"));
			//log.info("Metrics: "+metric.toString());
			//log.info("Tags: "+tagsList);
			if (topic1.equalsIgnoreCase(metric.getMetricName())) {
				toLeft = (int) metric.getValue();
			} else if (topic2.equalsIgnoreCase(metric.getMetricName())) {
				toRight = (int) metric.getValue();
			}

			for (String tag : tagsList) {
				//log.info("Tag,toRight: "+tag+","+toRight);
				k11.add(tag, toRight);
			}

			for (String tag :tagsList) {
				//log.info("Tag,toLeft: "+tag+","+toLeft);
				k12.add(tag, toLeft);
			}
			kx1 += toRight;
			kx2 += toLeft;
		}
		results.add("Tag: k11, k12, k21, k22, LogLikelihoodRatio");
		for (String tag : k11.elementSet()) {
			results.add(tag + ": " + k11.count(tag) + ", " + k12.count(tag) + ", " + (kx1 - k11.count(tag)) + ", "
					+ (kx2 - k12.count(tag)) + ", " + LogLikelihood.rootLogLikelihoodRatio(k11.count(tag),
							k12.count(tag), kx1 - k11.count(tag), kx2 - k12.count(tag)));
		}
		return results;
	}

	public static void getLLRHistogramData(int threshold, List<Observation> data, List<String> results) {
		Multiset<String> k11 = HashMultiset.create();
		Multiset<String> k12 = HashMultiset.create();
		int kx1 = 0;
		int kx2 = 0;
		results.add("Documents Size: " + data.size());
		for (Observation datum : data) {
			long[] counts = ((FloatHistogram) datum.getData()).getCounts();
			double[] centers = ((FloatHistogram) datum.getData()).getBounds();
			int toLeft = 0;
			int toRight = 0;
			for (int i = 0; i < centers.length; ++i) {
				if (centers[i] < threshold)
					toLeft = (int) (toLeft + counts[i]);
				else {
					toRight = (int) (toRight + counts[i]);
				}
			}
			List<String> tagsList = Arrays.asList(datum.getTags().trim().replaceAll("\\{|\\}", "").split("\\s*,\\s*"));
			for (String tag : tagsList) {
				k11.add(tag, toRight);
			}

			for (String tag : tagsList) {
				k12.add(tag, toLeft);
			}
			kx1 += toRight;
			kx2 += toLeft;
		}
		results.add("Tag: k11, k12, k21, k22, LogLikelihoodRatio");
		for (String tag : k11.elementSet())
			results.add(tag + ": " + k11.count(tag) + ", " + k12.count(tag) + ", " + (kx1 - k11.count(tag)) + ", "
					+ (kx2 - k12.count(tag)) + ", " + LogLikelihood.rootLogLikelihoodRatio(k11.count(tag),
							k12.count(tag), kx1 - k11.count(tag), kx2 - k12.count(tag)));
	}

	public static void printUsage() {
		System.out.println("==============================================================================================");
		System.out.println("Usage: com.mapr.spyglass.solution.QueryRequest <tableName> <tagKey> <fromDuration> <toDuration> <windowDurationInSecs> <threshold> [<tagk1=tagv1,tagk2=tagv2..>]\n");
		System.out.println("Example 1: com.mapr.spyglass.solution.QueryRequest /var/mapr/mapr.monitoring/histograms/ insert 1488322451462 1488913082690 60 1000 [op=insert]\n"
				+ "Example 2: com.mapr.spyglass.solution.QueryRequest /var/mapr/mapr.monitoring/histograms/ insert 1488322451462 now 60 20"
				+ "Example 3: com.mapr.spyglass.solution.QueryRequest /var/mapr/mapr.monitoring/histograms/ insert 1m-ago 1488913082690 60 10 [op=insert,fqdn=qa102-40.qa.lab]");
		System.out.println("==============================================================================================");
	}

}
