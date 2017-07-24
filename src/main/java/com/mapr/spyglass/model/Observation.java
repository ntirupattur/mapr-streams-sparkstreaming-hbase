package com.mapr.spyglass.model;

import java.io.Serializable;

public class Observation implements Serializable {
  /**
	 *
	 */
	private static final long serialVersionUID = 1L;
	String metricName;
	String tags;
  Object data;
  String hash;
  int windowDuration;
  int numOfDataPoints;
  long timeStamp;
  int hour, minute;
  public static enum type { floathistogram, tdigest, countarray}
  type objectType;


  public Observation(String metricName, String tags, Object data, String hash, int windowDuration, int numOfDataPoints, long timeStamp, int hour, int minute, Observation.type type) {
  	this.metricName = metricName;
  	this.tags = tags;
    this.data = data;
    this.hash = hash;
    this.numOfDataPoints = numOfDataPoints;
    this.windowDuration = windowDuration;
    this.timeStamp = timeStamp;
    this.hour = hour;
    this.minute = minute;
    this.objectType = type;
  }
  
  /**
   * @return the metricName
   */
  public String getMetricName() {
    return metricName;
  }

  /**
   * @return the windowDuration
   */
  public int getWindowDuration() {
    return windowDuration;
  }

  /**
   * @return the operationCount
   */
  public int getNumOfDataPoints() {
    return numOfDataPoints;
  }

  /**
   * @return the tags
   */
  public String getTags() {
    return tags;
  }

  /**
   * @return the histogram
   */
  public Object getData() {
    return data;
  }
 
	public long getTimeStamp() {
		return timeStamp;
	}

	public int getHour() {
		return hour;
	}

	public int getMinute() {
		return minute;
	}

	public type getObjectType() {
		return objectType;
	}

	public String getHash() {
		return hash;
	}
  
}

