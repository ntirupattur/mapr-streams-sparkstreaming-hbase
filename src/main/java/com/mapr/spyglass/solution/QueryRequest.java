/**
 * Class to query the data from JSON-DB tables
 */
package com.mapr.spyglass.solution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.SerializationUtils;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.QueryCondition;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import com.mapr.db.exceptions.TableNotFoundException;
import com.mapr.spyglass.model.Observation;
import com.tdunning.math.stats.FloatHistogram;


/**
 * @author ntirupattur
 *
 */
public class QueryRequest {
	/*
	 * Usage: com.mapr.spyglass.solution.QueryRequest <tableName> <tagKey> <fromDuration> <toDuration> <windowDurationInSecs> <threshold> [<tagk1=tagv1,tagk2=tagv2..>]
	 * Example 1: com.mapr.spyglass.solution.QueryRequest /var/mapr/mapr.monitoring/histograms/ insert 1488322451462 1488913082690 60 1000 [op=insert]
	 * Example 2: com.mapr.spyglass.solution.QueryRequest /var/mapr/mapr.monitoring/histograms/ insert 1488322451462 1488913082690 60 10
	 * Example 3: com.mapr.spyglass.solution.QueryRequest /var/mapr/mapr.monitoring/histograms/ insert 1488322451462 1488913082690 60 20 [op=insert,fqdn=qa102-40.qa.lab]
	 */
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
		String tags = "";

		if (args.length == 7) {
			tags = args[6];
		}

		List<String> results = performComputations(tableName, tagKey, fromDuration, toDuration, windowDurationInSec, threshold,
				tags);
		for (String result : results)
			System.out.println(result);
	}
	public static Map<String, FloatHistogram> getAggregatedHistogram(String tableName, String tagKey,
			String fromDuration, String toDuration, String windowDurationInSec, String tags) throws Exception {
		Map<String, FloatHistogram> histogramMap = new HashMap<String, FloatHistogram>();
		List<Document> documentsList = getHistogram(tableName, tagKey, fromDuration, toDuration, windowDurationInSec, tags);
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

	public static List<String> performComputations(String tableName, String tagKey, String fromDuration, String toDuration, String windowDurationInSec, String threshold, String tags) {
		List<String> results = new ArrayList<String>();
		try {
			List<Document> documentsList = getHistogram(tableName, tagKey, fromDuration, toDuration, windowDurationInSec, tags);
			List<Observation> observations = new ArrayList<Observation>();
			int count = 0;
			for (Document d : documentsList) {
				FloatHistogram histogram = (FloatHistogram) SerializationUtils
						.deserialize(Base64.decodeBase64(d.getString("histogram")));
				List<String> tagsList = Arrays.asList(d.getString("tags").trim().replaceAll("\\{|\\}", "").split("\\s*,\\s*"));
				String metricName = d.getString("metricname");
				int windowDuration = d.getInt("windowduration");
				int operationCount = d.getInt("count");
				count += operationCount;

				observations.add(new Observation(tagsList, histogram, metricName, windowDuration, operationCount));
			}

			results.add("Total Operations Count: " + count);
			getLLR(Integer.parseInt(threshold), observations, results);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return results;
	}

	public static List<Document> getHistogram(String tableName, String tagKey, String fromDuration, String toDuration, String windowDurationInSec, String tags) {
		List<Document> documentsList = new ArrayList<Document>();
		try {
			if (!(MapRDB.tableExists(tableName))) {
				throw new TableNotFoundException("Table: " + tableName + " does'nt exist");
			}

			Table table = MapRDB.getTable(tableName);

			QueryCondition condition = MapRDB.newCondition().and().matches("_id", tagKey)
					.is("timestamp", QueryCondition.Op.GREATER_OR_EQUAL,
							DateTime.parseDateTimeString(fromDuration, "PST")) // TODO - Get current timezone
					.is("timestamp", QueryCondition.Op.LESS_OR_EQUAL,
							((toDuration == null) || (toDuration.isEmpty()))
							? System.currentTimeMillis()
									: DateTime.parseDateTimeString(toDuration, "PST")) // TODO - Get current timezone
					.is("windowduration", QueryCondition.Op.EQUAL, Integer.parseInt(windowDurationInSec));

			if ((tags != null) && (!(tags.isEmpty()))) {
				QueryCondition optionalCondition = MapRDB.newCondition()
						.matches("hash", StringsUtil.getHashForTags(tags)).build();
				condition.condition(optionalCondition).close().build();
			} else {
				condition.close().build();
			}
			System.out.println("Condition: " + condition);
			DocumentStream docStream = table.find(condition);
			Iterator<Document> documentsIterator = docStream.iterator();
			while (documentsIterator.hasNext()) {
				Document d = (Document) documentsIterator.next();
				documentsList.add(d);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return documentsList;
	}

	public static void getLLR(int threshold, List<Observation> data, List<String> results) {
		Multiset<String> k11 = HashMultiset.create();
		Multiset<String> k12 = HashMultiset.create();
		int kx1 = 0;
		int kx2 = 0;
		results.add("Documents Size: " + data.size());
		for (Observation datum : data) {
			long[] counts = datum.getHistogram().getCounts();
			double[] centers = datum.getHistogram().getBounds();
			int toLeft = 0;
			int toRight = 0;
			for (int i = 0; i < centers.length; ++i) {
				if (centers[i] < threshold)
					toLeft = (int) (toLeft + counts[i]);
				else {
					toRight = (int) (toRight + counts[i]);
				}
			}

			for (String tag : datum.getTags()) {
				k11.add(tag, toRight);
			}

			for (String tag : datum.getTags()) {
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
		System.out.println("Usage: com.mapr.spyglass.solution.QueryRequest <tableName> <tagKey> <fromDuration> <toDuration> <threshold> [<tagk1=tagv1,tagk2=tagv2..>]\n");
		System.out.println("Example 1: com.mapr.spyglass.solution.QueryRequest /var/mapr/mapr.monitoring/histograms/ insert 1488322451462 1488913082690 60 1000 [op=insert]\n"
				+ "Example 2: com.mapr.spyglass.solution.QueryRequest /var/mapr/mapr.monitoring/histograms/ insert 1488322451462 now 60 20"
				+ "Example 3: com.mapr.spyglass.solution.QueryRequest /var/mapr/mapr.monitoring/histograms/ insert 1m-ago 1488913082690 60 10 [op=insert,fqdn=qa102-40.qa.lab]");
		System.out.println("==============================================================================================");
	}

}
