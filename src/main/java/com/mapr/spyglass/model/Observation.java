package com.mapr.spyglass.model;

import java.util.List;
import java.util.Set;

import com.tdunning.math.stats.FloatHistogram;

public class Observation {
  List<String> tags;
  FloatHistogram histogram;
  String metricName;
  int windowDuration;
  int operationCount;
  
  public Observation(List<String> tags, FloatHistogram histogram, String metricName, int windowDuration, int operationCount) {
    this.tags = tags;
    this.histogram = histogram;
    this.operationCount = operationCount;
    this.windowDuration = windowDuration;
    this.metricName = metricName;
  }
  
  /**
   * @return the metricName
   */
  public String getMetricName() {
    return metricName;
  }

  /**
   * @param metricName the metricName to set
   */
  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  /**
   * @return the windowDuration
   */
  public int getWindowDuration() {
    return windowDuration;
  }

  /**
   * @param windowDuration the windowDuration to set
   */
  public void setWindowDuration(int windowDuration) {
    this.windowDuration = windowDuration;
  }

  /**
   * @return the operationCount
   */
  public int getOperationCount() {
    return operationCount;
  }

  /**
   * @param operationCount the operationCount to set
   */
  public void setOperationCount(int operationCount) {
    this.operationCount = operationCount;
  }

  /**
   * @return the tags
   */
  public List<String> getTags() {
    return tags;
  }
  /**
   * @param tags the tags to set
   */
  public void setTags(List<String> tags) {
    this.tags = tags;
  }
  /**
   * @return the histogram
   */
  public FloatHistogram getHistogram() {
    return histogram;
  }
  /**
   * @param histogram the histogram to set
   */
  public void setHistogram(FloatHistogram histogram) {
    this.histogram = histogram;
  }
  
}

