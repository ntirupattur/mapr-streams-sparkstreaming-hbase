package com.mapr.spyglass.dao;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.mapr.spyglass.model.Observation;
import com.mapr.spyglass.solution.LogLikelihood;
import com.mapr.spyglass.solution.StringsUtil;

public class TestStrings {

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    String s = "op=insert,fqdn=qa102-40.qa.lab,app=xyz";

    try {
      //System.out.println(StringsUtil.getHashForTags(s));
      testLLR();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    //System.out.println(s.replaceAll("\\{|\\}", ""));
    //System.out.println(reverseWordsInString("i like this program very much"));


  }

  public static String reverseWordsInString(String input) {
    if(input == null || input.length() == 0 ) {
      return "";
    }
    StringBuilder output = new StringBuilder();
    for(int i =input.length()-1; i>=0;) {
      int j = i+1;
      while(input.charAt(i) != ' ' && i >=0) { // Bug here at charAt(i)
        i--;
      }
      output.append(input.substring(i+1,j));
      if (i > 0) {
        output.append(" ");
      }
      i--;

    }
    return output.toString();
  }

  private static class TestObservation {

    private Set<String> tags;
    private int toLeft, toRight;

    /**
     * @return the tags
     */
    public Set<String> getTags() {
      return tags;
    }
   
    /**
     * @return the toLeft
     */
    public int getToLeft() {
      return toLeft;
    }
   
    public int getToRight() {
      return toRight;
    }
   
    public TestObservation(Set<String> tags, int toLeft, int toRight) {
      this.tags = tags;
      this.toLeft = toLeft;
      this.toRight = toRight;
    }

  }

  public static void testLLR() {

    Multiset<String> k11 =  HashMultiset.create();
    Multiset<String> k12 =  HashMultiset.create();

    int kx1 = 0, kx2 = 0;
    
    List<TestObservation> data = new ArrayList<TestStrings.TestObservation>();
    
    Set<String> testSet = new HashSet<String>();
    testSet.add("t1=1");
    testSet.add("t2=b");
    
    Set<String> testSet1 = new HashSet<String>();
    testSet1.add("t1=2");
    testSet1.add("t2=b");
    
    Set<String> testSet2 = new HashSet<String>();
    testSet2.add("t1=3");
    testSet2.add("t2=b");
    
    data.add(new TestObservation(testSet, 10, 100));
    data.add(new TestObservation(testSet1, 20, 200));
    data.add(new TestObservation(testSet2, 30, 300));
    data.add(new TestObservation(testSet, 20, 100));
    
    for (TestObservation datum : data) {
      int toLeft = datum.getToLeft();
      int toRight = datum.getToRight();

      // inside region of interest
      for (String tag : datum.getTags()) {
        k11.add(tag, toLeft);
      }

      // outside region of interest
      for (String tag : datum.getTags()) {
        k12.add(tag, toRight);
      }
      kx1 += toLeft;
      kx2 += toRight;
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
