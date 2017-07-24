package com.mapr.spyglass.solution;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.ojai.Document;

import com.mapr.db.rowcol.DBList;
import com.mapr.spyglass.model.Observation;

public class AnomalyDetector {

	private static final Logger log = Logger.getLogger(AnomalyDetector.class);

	public static Callable<Double> findAnomalies(final String tableName, final Observation o) {

		Callable<Double> task = new Callable<Double>() {
			@Override
			public Double call() {
				log.info("Fetching the historic data");

				int[] currentCounts = (int[]) o.getData();
				int numOfDataPoints = o.getNumOfDataPoints();

				List<Document> documents = QueryRequest.getDocuments(tableName, o.getMetricName(), "1h-ago", "now", String.valueOf(o.getWindowDuration()), o.getTags(), null, null);
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
					historicDistribution[i] = (double)historicCounts[i]/numOfHistoricDataPoints;
					if (historicDistribution[i] == 0.0 || currentDistribution[i] == 0.0)
						relativeEntropy = relativeEntropy + 0.0;
					else
						relativeEntropy = relativeEntropy + (currentDistribution[i] * Math.log(currentDistribution[i]/historicDistribution[i]));
					//log.info("Current Distribution: "+currentDistribution[i]+" historic distribution: "+historicDistribution[i]+" relative entropy: "+relativeEntropy);
				}
					
				log.info("Relative entropy for the metric: "+o.getMetricName()+" with tags: "+o.getTags()+" is: "+relativeEntropy);
				return relativeEntropy;
			}
		};
		return task;
	}
}
