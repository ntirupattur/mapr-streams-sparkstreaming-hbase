/**
 * Class to query the data from JSON-DB tables
 */
package com.mapr.spyglass.solution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.SerializationUtils;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.QueryCondition;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import com.mapr.spyglass.model.Observation;
import com.sun.tools.javac.util.StringUtils;
import com.tdunning.math.stats.FloatHistogram;


/**
 * @author ntirupattur
 *
 */
public class QueryRequest {
  /*
   * Usage: com.mapr.spyglass.solution.QueryRequest <tableName> <tagKey> <windowDurationInSecs> <fromDuration> <toDuration> <threshold> [<tagk1=tagv1,tagk2=tagv2..>]
   * Example 1: com.mapr.spyglass.solution.QueryRequest /var/mapr/mapr.monitoring/histograms/ insert 1488322451462 1488913082690 60 1000 [op=insert]
   * Example 2: com.mapr.spyglass.solution.QueryRequest /var/mapr/mapr.monitoring/histograms/ insert 1488322451462 1488913082690 60 10
   * Example 3: com.mapr.spyglass.solution.QueryRequest /var/mapr/mapr.monitoring/histograms/ insert 1488322451462 1488913082690 60 20 [op=insert,fqdn=qa102-40.qa.lab]
   */
  public static void main(String[] args) {

    Table table;

    if (args.length < 6) {
      printUsage();
      return;
    }

    String tableName = args[0];
    String tagKey = args[1];
    String fromDuration = args[2]; //TODO - Accept values like 1m-ago, 1h-ago etc
    String toDuration = args[3];
    String windowDurationInSecs = args[4];
    String thresHold = args[5];
    String tags = "";

    if (args.length > 6) {
      if (args[6] != null && !args[6].isEmpty()) {
        tags = args[6];
      }
    }

    try {
      if (!MapRDB.tableExists(tableName)) {
        System.out.println("Table: "+tableName+ " does'nt exist");
        return;
      } else {
        table = MapRDB.getTable(tableName);
      }

      QueryCondition optionalCondition = MapRDB.newCondition()
          .matches("hash", StringsUtil.getHashForTags(tags))
          .build();



      QueryCondition condition = MapRDB.newCondition()
          .and()
          .matches("_id",  tagKey)
          .is("timestamp", QueryCondition.Op.GREATER_OR_EQUAL, Long.parseLong(fromDuration))
          .is("timestamp", QueryCondition.Op.LESS_OR_EQUAL, Long.parseLong(toDuration))
          .is("windowduration", QueryCondition.Op.EQUAL, Integer.parseInt(windowDurationInSecs));

      if (!tags.isEmpty()) {
        condition.condition(optionalCondition).close().build();
      } else {
        condition.close().build();
      }

      System.out.println("Condition: "+condition.toString());
      DocumentStream docStream = table.find(condition);
      Iterator<Document> documentsIterator = docStream.iterator();
      int count = 0;
      List<Observation> observations = new ArrayList<Observation>();
      while (documentsIterator.hasNext()) {
        Document d = documentsIterator.next();
        count++;

        FloatHistogram histogram = SerializationUtils.deserialize(Base64.decodeBase64(d.getString("histogram")));
        List<String> tagsList = Arrays.asList(d.getString("tags").trim().replaceAll("\\{|\\}", "").split("\\s*,\\s*"));
        String metricName = d.getString("metricname");
        int windowDuration = d.getInt("windowduration");
        int operationCount = d.getInt("count");

        observations.add(new Observation(tagsList, histogram, metricName, windowDuration, operationCount));
      }
      System.out.println("Total Count: "+count);
      getLLR(Integer.parseInt(thresHold), observations);
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Failed with exception: "+e.getMessage());
    }
  }

  public static void getLLR(int threshold, List<Observation> data) {

    Multiset<String> k11 =  HashMultiset.create();
    Multiset<String> k12 =  HashMultiset.create();

    int kx1 = 0, kx2 = 0;

    for (Observation datum : data) {
      long[] counts = datum.getHistogram().getCounts();
      double[] centers = datum.getHistogram().getBounds();
      int toLeft = 0, toRight = 0;
      for (int i = 0; i < centers.length; i++) {
        if (centers[i] < threshold) {
          toLeft += counts[i];
        } else {
          toRight += counts[i];
        }
      }

      // inside region of interest
      for (String tag : datum.getTags()) {
        k11.add(tag, toRight);
      }

      // outside region of interest
      for (String tag : datum.getTags()) {
        k12.add(tag, toLeft);
      }
      kx1 += toRight;
      kx2 += toLeft;
    }

    for (String tag : k11.elementSet()) {
      System.out.println("Tag ,   k11 ,   k12 ,   k21 ,   k22,   LogLikelihoodRatio");
      System.out.printf("%s,%6d,%6d,%6d,%6d,%6f\n", tag, k11.count(tag), k12.count(tag), kx1 - k11.count(tag), kx2 - k12.count(tag),
          LogLikelihood.rootLogLikelihoodRatio(k11.count(tag), k12.count(tag), kx1 - k11.count(tag), kx2 - k12.count(tag)));
      System.out.println();
    }
  }

  public static void printUsage() {
    System.out.println("==============================================================================================");
    System.out.println("Usage: com.mapr.spyglass.solution.QueryRequest <tableName> <tagKey> <fromDuration> <toDuration> <threshold> [<tagk1=tagv1,tagk2=tagv2..>]\n");
    System.out.println("Example 1: com.mapr.spyglass.solution.QueryRequest /var/mapr/mapr.monitoring/histograms/ insert 1488322451462 1488913082690 60 1000 [op=insert]\n"
        + "Example 2: com.mapr.spyglass.solution.QueryRequest /var/mapr/mapr.monitoring/histograms/ insert 1488322451462 1488913082690 60 20"
        + "Example 3: com.mapr.spyglass.solution.QueryRequest /var/mapr/mapr.monitoring/histograms/ insert 1488322451462 1488913082690 60 10 [op=insert,fqdn=qa102-40.qa.lab]");
    System.out.println("==============================================================================================");
  }

}
