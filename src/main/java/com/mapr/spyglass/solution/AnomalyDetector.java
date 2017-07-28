package com.mapr.spyglass.solution;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.ojai.Document;

import com.mapr.db.rowcol.DBList;
import com.mapr.spyglass.model.Observation;

public class AnomalyDetector {

	private static final Logger log = Logger.getLogger(AnomalyDetector.class);

	public static Callable<Double> calculateEntropy(final QueryRequest request, final Observation o, final String start, final String end, final String hour, final String minute) {

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
}
