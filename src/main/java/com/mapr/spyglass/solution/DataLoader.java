package com.mapr.spyglass.solution;

import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapr.spyglass.dao.MetricsDao;
import com.mapr.spyglass.model.Metric;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
public class DataLoader {

  private static final Logger log = Logger.getLogger(DataLoader.class);
  private static WebResource connection = null;
  private static Client client;
  private static ClientResponse response;
  private static ObjectMapper objMapper;

  /*
   * Usage: com.mapr.spyglass.DataLoader <opentsdb host> <tableName> <metricName> <tagk1=tagv1,tagk2=tagv2....> <fromDuration> <aggregateFunction>
   */
  public static void main(String[] args) {

    if (args.length != 6) {
      printUsage();
      return;
    }

    String hostName = args[0];
    String tableName = args[1];
    String metricName = args[2];
    String tags = args[3];
    String fromDuration = args[4];
    String aggregateFunction = args[5];
    // Convert the tags string to key,value pairs
    HashMap<String,String> tagsMap= new HashMap<String,String>();
    String[] tagPairs = Metric.splitString(tags, ',');
    for (int i=0;i<tagPairs.length;i++) {
      Metric.parse(tagsMap, tagPairs[i]);
    }
    
    String queryString = "";

    //http://10.10.10.82:4242/api/query?start=30d-ago&m=sum:cpu.percent{cpu_class=idle}

    try {
      
      queryString = "http://"+hostName+":4242/api/query?start="+URLEncoder.encode(fromDuration+
          "-ago", "UTF-8")+"&m="+URLEncoder.encode(aggregateFunction+":"+metricName+"{"+tags+"}","UTF-8");

      System.out.println("Query String: "+queryString);
      // Create a Jersey Client
      client = Client.create();

      // Create object mapper
      objMapper = new ObjectMapper();
      
      // Establish the connection
      connection = client.resource(queryString);

      response = connection.accept("application/json")
          .get(ClientResponse.class);
      
      if (response.getStatus() != 200) {
        throw new RuntimeException("Failed to fetch any data: HTTP error code : "
            + response.getStatus());
      }
      
      // Create the table to insert data into
      MetricsDao metricsDao = new MetricsDao(tableName);
      
      String jsonReponse = response.getEntity(String.class);

      // Get the response
      JsonNode rootNode = objMapper.readTree(jsonReponse);
      
      JsonNode dataPointsNode = rootNode.findPath("dps");
      
      //HashMap<String,Double> result = objMapper.readValue(dataPointsNode.toString(), HashMap.class);
      HashMap<String,Double> result = objMapper.convertValue(dataPointsNode, HashMap.class);
      
      System.out.println("Map size: "+result.size());
      int count = 0;
      for (Entry<String, Double> e:result.entrySet()) {
        if (count%1000==0) {
          System.out.println("Print count: "+count);
        }
        count++;
        Metric newMetric = new Metric();
        newMetric.setMetricName(metricName);
        newMetric.setTimeStamp(Long.parseLong(e.getKey()));
        Double temp;
        if (String.valueOf(e.getValue()).equals("0")) {
          temp = 0.0;
        } else {
          temp = e.getValue();
        }
        newMetric.setValue(temp);
        tagsMap.put("value", "aggregated");
        newMetric.setTags(tagsMap);
        boolean raiseAlarm = metricsDao.updateAggregates(newMetric,0,0,true);
      }
      

    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Failed with exception: "+e.getCause());
      log.error("Failed to get data for query: "+queryString+" with exception: "+e.getCause());
    }

  }

  public static void printUsage() {
    System.out.println("========================================================");
    System.out.println("Usage: com.mapr.spyglass.solution.DataLoader <opentsdb host> <tableName> <metricName> <tagk1=tagv1,tagk2=tagv2..> <fromDuration> <aggregateFunction>");
    System.out.println("========================================================");
  }
}
